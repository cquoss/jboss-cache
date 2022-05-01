/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.eviction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.ConfigureException;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeCacheListener;
import org.jboss.cache.optimistic.FqnComparator;
import org.jboss.cache.xml.XmlHelper;
import org.jgroups.View;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * Factory to create region from configuration, to track region,
 * and to resolve naming conflict for regions. Note that in addition to
 * user-specified regions, there is also a global cache <code>_default_</code>
 * region that covers everything else.
 *
 * @author Ben Wang 02-2004
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Id: RegionManager.java 3645 2007-03-06 21:18:29Z msurtani $
 */
public class RegionManager
{

   /**
    * Default region capacity.
    */
   public final static int CAPACITY = 200000;

   // There is global cache wide default values if no region is found.
   public final static Fqn DEFAULT_REGION = new Fqn("_default_");
   private static final Log log_ = LogFactory.getLog(RegionManager.class);

   private final Map regionMap_ = new ConcurrentHashMap();
   private int longestFqn = 0;

   private Timer evictionThread_;
   private EvictionTimerTask evictionTimerTask_;
   private int evictionThreadWakeupIntervalSeconds_;
   private TreeCache cache_;

   /**
    * @deprecated This is provided for JBCache 1.2 backwards API compatibility.
    */
   private EvictionPolicy policy_;

   public RegionManager()
   {
      evictionTimerTask_ = new EvictionTimerTask();
   }

   public int getEvictionThreadWakeupIntervalSeconds()
   {
      return evictionThreadWakeupIntervalSeconds_;
   }

   /**
    * @deprecated DO NOT USE THIS METHOD. IT IS PROVIDED FOR EJB3 INTEGRATION BACKWARDS COMPATIBILITY
    */
   public Region createRegion(String fqn, EvictionAlgorithm algorithm) throws RegionNameConflictException
   {
      return createRegion(Fqn.fromString(fqn), algorithm);
   }

   /**
    * @deprecated DO NOT USE THIS METHOD. IT IS PROVIDED FOR EJB3 INTEGRATION BACKWARDS COMPATIBILITY
    */
   public Region createRegion(Fqn fqn, EvictionAlgorithm algorithm) throws RegionNameConflictException
   {
      if (policy_ == null)
      {
         throw new RuntimeException("Deprecated API not properly setup for use. The RegionManager must be constructed with a single Policy");
      }

      EvictionConfiguration configuration;
      try
      {
         configuration =
               (EvictionConfiguration) policy_.getEvictionConfigurationClass().newInstance();

      }
      catch (RuntimeException re)
      {
         throw re;
      }
      catch (Exception e)
      {
         throw new RuntimeException("Could not instantiate EvictionConfigurationClass", e);
      }

      Region region = new Region(fqn, policy_, configuration);
      addRegion(fqn, region);
      evictionTimerTask_.addRegionToProcess(region);
      return region;
   }

   private void addRegion(Fqn fqn, Region region) throws RegionNameConflictException
   {
      checkConflict(fqn);
      regionMap_.put(fqn, region);
      longestFqn = Math.max(longestFqn, fqn.size());
   }

   /**
    * Create a region based on fqn.
    *
    * @param fqn          The region identifier.
    * @param regionConfig The XML configuration DOM Element for this region.
    * @throws RegionNameConflictException
    */
   public Region createRegion(String fqn, Element regionConfig)
         throws RegionNameConflictException
   {
      return createRegion(Fqn.fromString(fqn), regionConfig);
   }

   public Region createRegion(Fqn fqn, Element regionConfig)
         throws RegionNameConflictException
   {
      EvictionPolicy policy = this.createEvictionPolicy(regionConfig);
      EvictionConfiguration config = this.configureEvictionPolicy(policy, regionConfig);
      return this.createRegion(fqn, policy, config);
   }

   public Region createRegion(String fqn, EvictionPolicy policy, EvictionConfiguration config) throws RegionNameConflictException
   {
      return createRegion(Fqn.fromString(fqn), policy, config);
   }

   public Region createRegion(Fqn fqn, EvictionPolicy policy, EvictionConfiguration config) throws RegionNameConflictException
   {
      if (log_.isDebugEnabled())
      {
         log_.debug("createRegion(): creating region for fqn- " + fqn);
      }

      Region region = new Region(fqn, policy, config);
      addRegion(fqn, region);
      evictionTimerTask_.addRegionToProcess(region);
      return region;
   }

   public void configure(TreeCache cache)
   {
      if (log_.isTraceEnabled())
      {
         log_.trace("Configuring the eviction region manager");
      }
      cache_ = cache;

      // done for API 1.2 backwards compatibility for EJB3 integration.
      String evictionClass = cache_.getEvictionPolicyClass();
      if (evictionClass != null && evictionClass.length() != 0)
      {
         try
         {
            policy_ = (EvictionPolicy) loadClass(evictionClass).newInstance();
            policy_.configure(cache_);
            cache_.setEvictionPolicyProvider(policy_);
         }
         catch (Exception e)
         {
            log_.warn("Default Policy with class name " + evictionClass + " could not be loaded by the classloader");
         }
      }

      EvictionTreeCacheListener listener = new EvictionTreeCacheListener();
      cache_.setEvictionListener(listener);

      Element elem = cache_.getEvictionPolicyConfig();
      String temp = XmlHelper.getAttr(elem,
            EvictionConfiguration.WAKEUP_INTERVAL_SECONDS, EvictionConfiguration.ATTR, EvictionConfiguration.NAME);

      if (temp == null)
      {
         evictionThreadWakeupIntervalSeconds_ = EvictionConfiguration.WAKEUP_DEFAULT;
      }
      else
      {
         evictionThreadWakeupIntervalSeconds_ = Integer.parseInt(temp);
      }

      if (evictionThreadWakeupIntervalSeconds_ <= 0)
         evictionThreadWakeupIntervalSeconds_ = EvictionConfiguration.WAKEUP_DEFAULT;

      NodeList list = elem.getElementsByTagName(EvictionConfiguration.REGION);
      for (int i = 0; i < list.getLength(); i++)
      {
         org.w3c.dom.Node node = list.item(i);
         if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
            continue;
         Element element = (Element) node;
         String name = element.getAttribute(EvictionConfiguration.NAME);
         try
         {
            this.createRegion(name, element);
         }
         catch (RegionNameConflictException e)
         {
            throw new RuntimeException(
                  "Illegal region name specified for eviction policy " + name
                        + " exception: " + e);
         }

      }

   }

   /**
    * Removes a region by string.
    */
   public void removeRegion(String fqn)
   {
      removeRegion(Fqn.fromString(fqn));
   }

   /**
    * Removes a region by Fqn, returns true if the region was found.
    */
   public boolean removeRegion(Fqn fqn)
   {
      Region region = (Region) regionMap_.remove(fqn);
      evictionTimerTask_.removeRegionToProcess(region);
      return region != null;
   }

   /**
    * Returns true if the region exists.
    */
   public boolean hasRegion(String myFqn)
   {
      return hasRegion(Fqn.fromString(myFqn));
   }

   /**
    * Returns true if the region exists.
    */
   public boolean hasRegion(Fqn fqn)
   {
      return regionMap_.containsKey(fqn);
   }

   /**
    * Returns the Region belonging to a String FQN.
    */
   public Region getRegion(String myFqn)
   {
      return getRegion(Fqn.fromString(myFqn));
   }

   /**
    * Returns the Region corresponding to this Fqn.
    */
   public Region getRegion(Fqn fqn)
   {
      // Truncate Fqn as an optimization
      if (fqn.size() > longestFqn)
         fqn = fqn.getFqnChild(0, longestFqn);

      Region region;
      while (!fqn.isRoot())
      {
         region = (Region) regionMap_.get(fqn);
         if (region != null)
            return region;
         fqn = fqn.getParent();
      }

      if (log_.isTraceEnabled())
      {
         log_.trace("getRegion(): user-specified region not found: " + fqn
               + " will use the global default region");
      }
      region = (Region) regionMap_.get(DEFAULT_REGION);
      if (region == null)
      {
         throw new RuntimeException(
               "RegionManager.getRegion(): Default region (" + DEFAULT_REGION + ") is not configured!" +
                     " You will need to define it in your EvictionPolicyConfig.");
      }
      return region;
   }

   /**
    * Returns an ordered list of regions.
    * Orders (by reverse) Fqn.
    */
   public Region[] getRegions()
   {
       Set s = new TreeSet(new RegionComparator());
        for (Iterator i = regionMap_.values().iterator(); i.hasNext();) s.add(i.next());

        return (Region[]) s.toArray(new Region[]{});
   }

   /**
    * Check for conflict in the current regions. There is a conflict
    * if fqn is any parent fqn of the current regions.
    *
    * @param myFqn Current fqn for potential new region.
    * @throws RegionNameConflictException to indicate a region name conflict has ocurred.
    */
   public void checkConflict(String myFqn) throws RegionNameConflictException
   {
      checkConflict(Fqn.fromString(myFqn));
   }

   /**
    * Check for conflict in the current regions. There is a conflict
    * if fqn is any parent fqn of the current regions.
    */
   public void checkConflict(Fqn fqn) throws RegionNameConflictException
   {
      for (int i = 0; i < fqn.size(); i++)
      {
         Fqn child = fqn.getFqnChild(i);
         if (regionMap_.containsKey(child))
         {
            throw new RegionNameConflictException("RegionManager.checkConflict(): new region fqn "
                  + fqn + " is in conflict with existing region fqn " + child);
         }
      }
      // We are clear then.
   }

   /**
    * Mark a node as currently in use.
    *
    * @param fqn Fqn of the node.
    */
   public void markNodeCurrentlyInUse(Fqn fqn, long timeout)
   {
      Region region = this.getRegion(fqn);
      EvictedEventNode markUse = new EvictedEventNode(fqn, EvictedEventNode.MARK_IN_USE_EVENT);
      markUse.setInUseTimeout(timeout);
      region.putNodeEvent(markUse);
   }

   /**
    * Unmark a node currently in use.
    *
    * @param fqn Fqn of the node.
    */
   public void unmarkNodeCurrentlyInUse(Fqn fqn)
   {
      Region region = this.getRegion(fqn);
      EvictedEventNode markNoUse = new EvictedEventNode(fqn, EvictedEventNode.UNMARK_USE_EVENT);
      region.putNodeEvent(markNoUse);
   }

   /**
    * This method is here to aid unit testing.
    */
   final void setTreeCache(TreeCache cache)
   {
      this.cache_ = cache;
   }

   private EvictionConfiguration configureEvictionPolicy(EvictionPolicy policy, Element regionConfig)
   {
      try
      {
         EvictionConfiguration configuration =
               (EvictionConfiguration) policy.getEvictionConfigurationClass().newInstance();
         configuration.parseXMLConfig(regionConfig);
         return configuration;
      }
      catch (RuntimeException re)
      {
         throw re;
      }
      catch (ConfigureException e)
      {
         throw new RuntimeException("Error configuring Eviction Policy", e);
      }
      catch (Exception e)
      {
         throw new RuntimeException("Eviction configuration class is not properly loaded in classloader", e);
      }
   }

   private EvictionPolicy createEvictionPolicy(Element regionConfig)
   {

      // use the eviction policy class specified per region or use the one specified for the entire cache.
      // this maintains JBCache configuration backwards compatibility. The old style configuration
      // where there is only one Policy for all Regions will still work. We can also now support
      // a different policy per Region (new style configuration - version 1.3+).
      String evictionClass = regionConfig.getAttribute(EvictionConfiguration.REGION_POLICY_CLASS);
      if (evictionClass == null || evictionClass.length() == 0)
      {
         evictionClass = cache_.getEvictionPolicyClass();
         // if it's still null... what do we configure?
         if (evictionClass == null || evictionClass.length() == 0)
         {
            throw new RuntimeException(
                  "There is no Eviction Policy Class specified on the region or for the entire cache!");
         }

//         return this.policy_;
      }

      try
      {
         if (log_.isTraceEnabled())
         {
            log_.trace("Creating policy " + evictionClass);
         }
         EvictionPolicy policy = (EvictionPolicy) loadClass(evictionClass).newInstance();
         policy.configure(cache_);
         return policy;
      }
      catch (Exception e)
      {
         throw new RuntimeException("Eviction class is not properly loaded in classloader", e);
      }
   }

   public static boolean isUsingNewStyleConfiguration(Element evictionRegionConfigElem)
   {
      if (evictionRegionConfigElem == null)
      {
         return false;
      }

      NodeList list = evictionRegionConfigElem.getElementsByTagName(EvictionConfiguration.REGION);
      for (int i = 0; i < list.getLength(); i++)
      {
         org.w3c.dom.Node node = list.item(i);
         if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
            continue;
         Element element = (Element) node;
         String evictionClass = element.getAttribute(EvictionConfiguration.REGION_POLICY_CLASS);
         if (evictionClass != null && evictionClass.trim().length() > 0)
         {
            return true;
         }
      }

      return false;
   }

   public String toString()
   {
      return super.toString() + " regions=" + regionMap_;
   }

   class EvictionTreeCacheListener implements TreeCacheListener
   {
      public void nodeCreated(Fqn fqn)
      {
      }

      public void nodeRemoved(Fqn fqn)
      {
      }

      public void nodeLoaded(Fqn fqn)
      {
      }

      public void nodeEvicted(Fqn fqn)
      {
      }

      public void nodeModified(Fqn fqn)
      {
      }

      public void nodeVisited(Fqn fqn)
      {
      }

      public void cacheStarted(TreeCache cache)
      {
         if (log_.isDebugEnabled()) log_.debug("Starting eviction timer");
         evictionThread_ = new Timer();
         evictionThread_.schedule(evictionTimerTask_, RegionManager.this.getEvictionThreadWakeupIntervalSeconds() * 1000,
               RegionManager.this.getEvictionThreadWakeupIntervalSeconds() * 1000);
      }

      public void cacheStopped(TreeCache cache)
      {
         if (log_.isDebugEnabled()) log_.info("Stopping eviction timer ... ");
         if (evictionThread_ != null)
            evictionThread_.cancel();
         evictionThread_ = null;
      }

      public void viewChange(View new_view)  // might be MergeView after merging
      {
      }
   }

   static class RegionComparator implements Comparator
   {
      public int compare(Object o1, Object o2)
      {
         Region r1 = (Region) o1;
         Region r2 = (Region) o2;
         Fqn f1 = r1.getFqnObject();
         Fqn f2 = r2.getFqnObject();
         if (f1.equals(DEFAULT_REGION)) return -1;
         if (f2.equals(DEFAULT_REGION)) return 1;
         return -FqnComparator.INSTANCE.compare(f1, f2);
      }
   }

   private static Class loadClass(String classname) throws ClassNotFoundException
   {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null)
         cl = ClassLoader.getSystemClassLoader();
      return cl.loadClass(classname);
   }

}
