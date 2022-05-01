/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.config.Option;
import org.jboss.cache.aop.util.ObjectUtil;

import java.util.Map;
import java.util.Iterator;

/**
 * PojoCache delegation to handle internal cache sotre, that is, the portion that is not part of user's data.
 *
 * @author Ben Wang
 */
public class InternalDelegate
{
   static Log log=LogFactory.getLog(InternalDelegate.class.getName());
   public static final String CLASS_INTERNAL = "__jboss:internal:class__";
   public static final String SERIALIZED = "__SERIALIZED__";
   public static final Fqn JBOSS_INTERNAL = new Fqn("__JBossInternal__");
   public static final Fqn JBOSS_INTERNAL_MAP = new Fqn(JBOSS_INTERNAL, "__RefMap__");
   // This is an optimization flag to skip put lock when we are sure that it has been locked from
   // putObject. However, if later on there are transactional field updates, then we we will need
   // this off to protected the write lock.
   private boolean cacheOperationSkipLocking = true;
   private Option skipLockOption_;
   private Option gravitateOption_;
   private Option localModeOption_;

   protected PojoCache cache_;

   InternalDelegate(PojoCache cache)
   {
      cache_ = cache;

      skipLockOption_ = new Option();
      if(cacheOperationSkipLocking)
      {
         skipLockOption_.setSuppressLocking(true);
      } else
      {
         skipLockOption_.setSuppressLocking(false);
      }

      localModeOption_ = new Option();
      localModeOption_.setCacheModeLocal(true);

      gravitateOption_ = new Option();
      gravitateOption_.setForceDataGravitation(true);
   }

   Option getLockOption()
   {
      return skipLockOption_;
   }

   protected AOPInstance getAopInstance(Fqn fqn) throws CacheException
   {
      // Not very efficient now since we are peeking every single time.
      // Should have cache it without going to local cache.
      return (AOPInstance)get(fqn, AOPInstance.KEY, false);
   }

   protected AOPInstance getAopInstanceWithGravitation(Fqn fqn) throws CacheException
   {
      // Not very efficient now since we are peeking every single time.
      // Should have cache it without going to local cache.
      return (AOPInstance)get(fqn, AOPInstance.KEY, true);
   }

   AOPInstance initializeAopInstance(Fqn fqn) throws CacheException
   {
      AOPInstance aopInstance = new AOPInstance();

      aopInstance.incrementRefCount(null);
      return aopInstance;
   }

   /**
    * Increment reference count for the pojo. Note that this is not thread safe or atomic.
    */
   int incrementRefCount(Fqn originalFqn, Fqn referencingFqn) throws CacheException
   {
       AOPInstance aopInstanceOrig = getAopInstance(originalFqn);
       if(aopInstanceOrig == null)
          throw new RuntimeException("InternalDelegate.incrementRefCount(): null aopInstance for fqn: " +originalFqn);
       AOPInstance aopInstance = aopInstanceOrig.copy();

      int count = aopInstance.incrementRefCount(referencingFqn);
      // need to update it.
      put(originalFqn, AOPInstance.KEY, aopInstance);
      return count;
   }

   /**
    * Has a delegate method so we can use the switch.
    */

   protected Object get(Fqn fqn, Object key) throws CacheException
   {
      return get(fqn, key, false);
   }

   protected Object get(Fqn fqn, Object key, boolean gravitate) throws CacheException
   {
      // TODO let's find a better way to decouple this.
      if (gravitate && cache_.getBuddyManager() != null)
      {
         return cache_.get(fqn, key, gravitateOption_);
      }
      else if (cache_.getCacheLoader() != null)
      {
         // We have cache loader, we can't get it directly from the local get.
         return cache_.get(fqn, key, skipLockOption_);
      } else
      {
         return cache_._get(fqn, key, false);
      }
   }

   protected void put(Fqn fqn, Object key, Object value) throws CacheException
   {
      // Use option to ski locking since we have parent lock already.
      cache_.put(fqn, key, value, skipLockOption_);
//      cache_.put(fqn, key, value);
   }

   protected void put(Fqn fqn, Map map) throws CacheException
   {
      // Use option to ski locking since we have parent lock already.
      cache_.put(fqn, map, skipLockOption_);
//      cache_.put(fqn, key, value);
   }

   protected void localPut(Fqn fqn, Object key, Object value) throws CacheException
   {
      // Use option to ski locking since we have parent lock already.
      // TODO Need to make sure there is no tx here otherwise it won't work.
      cache_.put(fqn, key, value, localModeOption_);
   }


   /**
    * decrement reference count for the pojo. Note that this is not thread safe or atomic.
    */
   int decrementRefCount(Fqn originalFqn, Fqn referencingFqn) throws CacheException
   {
       AOPInstance aopInstanceOrig = getAopInstance(originalFqn);
       if(aopInstanceOrig == null)
          throw new RuntimeException("InternalDelegate.decrementRefCount(): null aopInstance.");
       AOPInstance aopInstance = aopInstanceOrig.copy();

      int count = aopInstance.decrementRefCount(referencingFqn);

      if(count < -1)  // can't dip below -1
         throw new RuntimeException("InternalDelegate.decrementRefCount(): null aopInstance.");

      // need to update it.
      put(originalFqn, AOPInstance.KEY, aopInstance);
      return count;
   }

   boolean isReferenced(AOPInstance aopInstance, Fqn fqn) throws CacheException
   {
      // If ref counter is greater than 0, we fqn is being referenced.
      return (aopInstance.getRefCount() > 0);
   }

   int getRefCount(Fqn fqn) throws CacheException
   {
      return getAopInstance(fqn).getRefCount();
   }

   String getRefFqn(Fqn fqn) throws CacheException {
      AOPInstance aopInstance = getAopInstance(fqn);
      return getRefFqn(aopInstance, fqn);
   }

   String getRefFqn(AOPInstance aopInstance, Fqn fqn) throws CacheException
   {
      if(aopInstance == null)
         return null;

      String aliasFqn = aopInstance.getRefFqn();

      if(aliasFqn == null || aliasFqn.length() == 0) return null;

      return getRefFqnFromAlias(aliasFqn);
   }

   void setRefFqn(Fqn fqn, String internalFqn) throws CacheException
   {
       AOPInstance aopInstanceOrig = getAopInstance(fqn);
       AOPInstance aopInstance = null;
       if(aopInstanceOrig == null)
          aopInstance = new AOPInstance();
       else
           aopInstance = aopInstanceOrig.copy();

      aopInstance.setRefFqn(internalFqn);
      put(fqn, AOPInstance.KEY, aopInstance);
   }

   void removeRefFqn(Fqn fqn) throws CacheException
   {
       AOPInstance aopInstanceOrig = getAopInstance(fqn);
       if(aopInstanceOrig == null)
          throw new RuntimeException("InternalDelegate.getRefFqn(): null aopInstance.");
       AOPInstance aopInstance = aopInstanceOrig.copy();

      aopInstance.removeRefFqn();
      put(fqn, AOPInstance.KEY, aopInstance);
   }

   Object getPojo(Fqn fqn) throws CacheException
   {
      AOPInstance aopInstance = getAopInstance(fqn);
      if(aopInstance == null)
         return null;

      return aopInstance.get();
   }

   Object getPojoWithGravitation(Fqn fqn) throws CacheException
   {
      // This is for buddy replication
      AOPInstance aopInstance = getAopInstanceWithGravitation(fqn);
      if(aopInstance == null)
         return null;

      return aopInstance.get();
   }

   void setPojo(Fqn fqn, Object pojo) throws CacheException
   {
      AOPInstance aopInstance = getAopInstance(fqn);
      if(aopInstance == null)
      {
         aopInstance = new AOPInstance();
         put(fqn, AOPInstance.KEY, aopInstance);
      }

      aopInstance.set(pojo);
      // No need to do a cache put since pojo is transient anyway.
   }

   void setPojo(AOPInstance aopInstance, Object pojo) throws CacheException
   {
      // No need to do a cache put since pojo is transient anyway.
      aopInstance.set(pojo);
   }

   void setPojo(Fqn fqn, Object pojo, AOPInstance aopInstance) throws CacheException
   {
      if(aopInstance == null)
      {
         aopInstance = new AOPInstance();
         put(fqn, AOPInstance.KEY, aopInstance);
      }

      aopInstance.set(pojo);
      // No need to do a cache put since pojo is transient anyway.
   }

   /**
    * We store the class name in string.
    */
   void putAopClazz(Fqn fqn, Class clazz) throws CacheException {
      put(fqn, CLASS_INTERNAL, clazz);
   }

   /**
    * We store the class name in string and put it in map instead of directly putting
    * it into cache for optimization.
    */
   void putAopClazz(Fqn fqn, Class clazz, Map map) throws CacheException {
      map.put(CLASS_INTERNAL, clazz);
   }

   Class peekAopClazz(Fqn fqn) throws CacheException {
      return (Class)get(fqn, CLASS_INTERNAL);
   }

   boolean isAopNode(Fqn fqn) throws CacheException
   {
      // Use this API so it doesn't go thru the interceptor.
      DataNode node = cache_.peek(fqn);
      if(node == null) return false;

      if( node.get(AOPInstance.KEY) != null )
         return true;
      else
         return false;
   }

   void removeInternalAttributes(Fqn fqn) throws CacheException
   {
      cache_.remove(fqn, AOPInstance.KEY);
      cache_.remove(fqn, CLASS_INTERNAL);
   }

   void cleanUp(Fqn fqn, boolean evict) throws CacheException
   {
      // We can't do a brute force remove anymore?
      if(!evict)
      {
         Map children = cache_._get(fqn).getChildren();
         if (children == null || children.size() == 0)
         {
            // remove everything
            cache_.remove(fqn);
         } else
         {
            // Assume everything here is all PojoCache data for optimization
            cache_.removeData(fqn);
            if (log.isTraceEnabled()) {
               log.trace("cleanup(): fqn: " + fqn + " is not empty. That means it has sub-pojos. Will not remove node");
            }
         }
      } else
      {
         // This has to use plainEvict method otherwise it is recursively calling aop version of evict.
         cache_.plainEvict(fqn);
      }
   }

   String createIndirectFqn(String fqn) throws CacheException
   {
      String indirectFqn = getIndirectFqn(fqn);
      Fqn internalFqn = getInternalFqn(fqn);
      put(internalFqn, indirectFqn, fqn);
      return indirectFqn;
   }

   Fqn getInternalFqn(String fqn)
   {
      if(fqn == null || fqn.length() == 0)
         throw new IllegalStateException("InternalDelegate.getInternalFqn(). fqn is either null or empty!");

      String indirectFqn = getIndirectFqn(fqn);
      return new Fqn(JBOSS_INTERNAL_MAP, indirectFqn);
//      return JBOSS_INTERNAL_MAP;
   }

   String getIndirectFqn(String fqn)
   {
      // TODO This is not unique. Will need to come up with a better one in the future.
      return ObjectUtil.getIndirectFqn(fqn);
   }

   void removeIndirectFqn(String oldFqn) throws CacheException
   {
      String indirectFqn = getIndirectFqn(oldFqn);
      cache_.remove(getInternalFqn(oldFqn), indirectFqn);
   }

   void setIndirectFqn(String oldFqn, String newFqn) throws CacheException
   {
      String indirectFqn = getIndirectFqn(oldFqn);
      Fqn tmpFqn = getInternalFqn(oldFqn);
      if(cache_.exists(tmpFqn, indirectFqn)) // No need to update if it doesn't exist.
      {
         put(tmpFqn, indirectFqn, newFqn);
      }
   }

   void updateIndirectFqn(Fqn originalFqn, Fqn newFqn) throws CacheException
   {
      put(getInternalFqn(originalFqn.toString()), getIndirectFqn(originalFqn.toString()), newFqn.toString());
   }

   String getRefFqnFromAlias(String aliasFqn) throws CacheException
   {
      return (String)get(getInternalFqn(aliasFqn), aliasFqn, true);
   }

   Fqn getNextFqnInLine(Fqn currentFqn) throws CacheException
   {
      AOPInstance ai = getAopInstance(currentFqn);
      return ai.getAndRemoveFirstFqnInList();
   }

   void relocate(Fqn thisFqn, Fqn newFqn) throws CacheException
   {
      /**
      DataNode node = cache_.get(thisFqn);
      DataNode newParent = (DataNode)cache_.get(newFqn).getParent();
      node.relocate(newParent, newFqn); // relocation
      updateIndirectFqn(thisFqn, newFqn);
       */

      // Let's do cache-wide copy then. It won't be fast and atomic but
      // at least it preserves the pojo structure and also do replication
      // TODO Can TreeCache provide a method to do this??

      // First do a recursive copy using the new base fqn
      DataNode node = cache_.get(thisFqn);
      Map value = node.getData();
      cache_.put(newFqn, value);

      Map children = node.getChildren();
      if(children == null || children.size() == 0)
      {
         cache_.remove(thisFqn);
         return; // we are done
      }

      Iterator it = children.keySet().iterator();
      while(it.hasNext())
      {
         Object key = it.next();
         Fqn thisChildFqn = new Fqn(thisFqn, key);
         Fqn newChildFqn = new Fqn(newFqn, key);
         relocate(thisChildFqn, newChildFqn);
      }

      // Finally do a remove
      cache_.remove(thisFqn);
   }

   /**
    * Test if this internal node.
    * @param fqn
    */
   public static boolean isInternalNode(Fqn fqn) {
      // we ignore all the node events corresponding to JBOSS_INTERNAL
      if(fqn.isChildOrEquals(InternalDelegate.JBOSS_INTERNAL)) return true;

      return false;
   }
}
