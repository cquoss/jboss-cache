/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeNode;
import org.jboss.cache.eviction.EvictedEventNode;
import org.jboss.cache.eviction.Region;
import org.jboss.cache.eviction.RegionManager;
import org.jboss.cache.loader.CacheLoader;
import org.jgroups.util.Util;

public class StateTransferIntegrator_123 implements StateTransferIntegrator
{
   private Log log = LogFactory.getLog(getClass().getName());
   
   private byte[] transient_state;
   private byte[] persistent_state;
   private TreeCache cache;
   
   StateTransferIntegrator_123(byte[] state, Fqn targetFqn, TreeCache cache)
   {
      if (targetFqn.size() > 0)
      {
         throw new IllegalArgumentException("Invalid FQN " + targetFqn + 
                                            " -- StateTransferVersion 123 only supports " +
                                            "transferring  FQN '/'");  
      }
      
      this.cache = cache;
      
      byte[][] states=null;
      
      try 
      {
         log.info("received the state (size=" + state.length + " bytes)");
         states=(byte[][])Util.objectFromByteBuffer(state);
         transient_state=states[0];
         persistent_state=states[1];
         if(transient_state != null)
            log.info("transient state: " + transient_state.length + " bytes");
         if(persistent_state != null)
            log.info("persistent state: " + persistent_state.length + " bytes");
      }
      catch(Throwable t) 
      {
         log.error("failed unserializing state", t);
      }
   }
   
   public void integrateTransientState(DataNode target, ClassLoader cl) throws Exception
   {
      // Clear the target
      target.clear();
      target.removeAllChildren();
      
      if(transient_state != null) 
      {
         try 
         {
            log.info("setting transient state");
            Node new_root = (Node) Util.objectFromByteBuffer(transient_state);
            
            // Integrate the deserialized data and children into the target
            Map data = new_root.getData();
            target.put(data, true);
            Map children = new_root.getChildren();
            if (children != null)
            {
               for (Iterator iter = children.entrySet().iterator(); iter.hasNext();)
               {
                  Map.Entry entry = (Map.Entry) iter.next();
                  TreeNode child = (TreeNode) entry.getValue();
                  target.addChild(entry.getKey(), child);
               }
            }
            
            target.setRecursiveTreeCacheInstance(cache);  // need to set this at root and set it recursively
            
            if (children != null)
            {
               RegionManager erm = cache.getEvictionRegionManager();
               if (erm != null)
               {
                  Region[] regions = erm.getRegions();
                  if (regions == null || regions.length == 0)
                     erm = null;
               }
               
               if (erm != null)
               {
                  for (Iterator iter = children.values().iterator(); iter.hasNext();)
                  {
                     sendEvictionNotifications((TreeNode) iter.next(), erm);
                  }
               }
            }
               
            log.info("setting the transient state was successful");
         }
         catch(Throwable t) 
         {
            log.error("failed setting transient state", t);
         }
      }
   }

   public void integratePersistentState() throws Exception
   {
      if(persistent_state != null) 
      {
         CacheLoader cache_loader = cache.getCacheLoader();
         if(cache_loader == null) 
         {
            log.error("cache loader is null, cannot set persistent state");
         }
         else 
         {
            try 
            {
               log.info("setting the persistent state");
               // cache_loader.remove(Fqn.fromString("/"));
               cache_loader.storeEntireState(persistent_state);
               log.info("setting the persistent state was successful");
            }
            catch(Throwable t) 
            {
               log.error("failed setting persistent state", t);
            }
         }
      }
   }
   
   private void sendEvictionNotifications(TreeNode node, RegionManager erm)
   {
      Fqn fqn = node.getFqn();      

      Region region = null;
      try
      {
         region = erm.getRegion(fqn);
      }
      catch (RuntimeException e)
      {
         if (erm.hasRegion(RegionManager.DEFAULT_REGION))
            throw e;
         // else the fqn is not associated with an eviction region
      }
      
      if (region != null)
      {
         Map attrs = node.getData();
         region.putNodeEvent(new EvictedEventNode(fqn, EvictedEventNode.ADD_NODE_EVENT, 
                                                  attrs == null ? 0 : attrs.size()));
      }
      
      // Walk the tree
      Map children = node.getChildren();
      if (children != null)
      {
         for (Iterator iter = children.values().iterator(); iter.hasNext();)
         {
            sendEvictionNotifications((TreeNode) iter.next(), erm);
         }
      }
   }

}
