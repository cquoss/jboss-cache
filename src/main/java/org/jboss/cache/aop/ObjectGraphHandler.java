/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop;

import org.jboss.cache.Fqn;
import org.jboss.cache.CacheException;
import org.jboss.cache.aop.util.AopUtil;
import org.jboss.cache.aop.collection.AbstractCollectionInterceptor;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.InstanceAdvisor;
import org.jboss.aop.Advised;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Handle the object graph management.
 *
 * @author Ben Wang
 *         Date: Aug 4, 2005
 * @version $Id: ObjectGraphHandler.java 1713 2006-04-25 19:26:08Z genman $
 */
public class ObjectGraphHandler {
   protected PojoCache cache_;
   protected InternalDelegate internal_;
   protected final static Log log=LogFactory.getLog(ObjectGraphHandler.class);
   protected TreeCacheAopDelegate delegate_;

   public ObjectGraphHandler(PojoCache cache, InternalDelegate internal, TreeCacheAopDelegate delegate)
   {
      cache_ = cache;
      internal_ = internal;
      delegate_ = delegate;
   }

   Object objectGraphGet(Fqn fqn) throws CacheException
   {
      // Note this is actually the aliasFqn, not the real fqn!
      String refFqn = internal_.getRefFqn(fqn);
      Object obj;
      if (refFqn != null) {
         // this is recursive. Need to obtain the object from parent fqn
         // No need to add CacheInterceptor as a result. Everything is re-directed.
         // In addition, this op will not be recursive.
         if (log.isDebugEnabled()) {
            log.debug("getObject(): obtain value from reference fqn: " + refFqn);
         }
         obj = cache_.getObject(refFqn);
         if(obj == null)
            throw new RuntimeException("ObjectGraphHandler.objectGraphGet(): null object from internal ref node." +
                    " Original fqn: " +fqn + " Internal ref node: " +refFqn);

         return obj; // No need to set the instance under fqn. It is located in refFqn anyway.
      }

      return null;
   }

   boolean objectGraphPut(Fqn fqn, Interceptor interceptor, CachedType type, Object obj) throws CacheException
   {
      Fqn originalFqn = null;
      if (interceptor == null) {
         return false;  // No interceptor no object graph possibility.
      }

      if(interceptor instanceof AbstractCollectionInterceptor)
      {
         // Special case for Collection class. If it is detached, we don't care.
         if( !((AbstractCollectionInterceptor)interceptor).isAttached())
         {
            return false;
         }
      }
      // ah, found something. So this will be multiple referenced.
      originalFqn = ((BaseInterceptor) interceptor).getFqn();

      if(originalFqn == null) return false;

      if (log.isDebugEnabled()) {
         log.debug("handleObjectGraph(): fqn: " + fqn + " and " + originalFqn + " share the object.");
      }

      // This will increment the ref count, reset, and add ref fqn in the current fqn node.
      setupRefCounting(fqn, originalFqn);
      internal_.putAopClazz(fqn, type.getType());
      return true;
   }

   boolean objectGraphRemove(Fqn fqn, boolean removeCacheInterceptor, Object pojo, boolean evict)
           throws CacheException
   {
      boolean isTrue = false;

      // Note this is actually the aliasFqn, not the real fqn!
      AOPInstance aopInstance = internal_.getAopInstance(fqn);
      String refFqn = internal_.getRefFqn(aopInstance, fqn);
      // check if this is a refernce
      if (refFqn != null) {
         if (log.isDebugEnabled()) {
            log.debug("objectGraphRemove(): removing object fqn: " + fqn + " but is actually from ref fqn: " + refFqn
            + " Will just de-reference it.");
         }
         removeFromReference(fqn, refFqn, removeCacheInterceptor, evict);
         internal_.cleanUp(fqn, evict);
         isTrue = true;
      } else {
         if(internal_.isReferenced(aopInstance, fqn))
         {
            // This node is currently referenced by others. We will relocate it to the next in line,
            // and update the indirectFqnMap

            // First decrement counter.
            decrementRefCount(fqn, null);
            // Determine where to move first.
            Fqn newFqn = internal_.getNextFqnInLine(fqn);
            // Is newFqn is child of fqn?
            if(newFqn.isChildOf(fqn))
            {
               // Take out the child fqn reference to me.
               internal_.removeRefFqn(newFqn);

               if (log.isDebugEnabled()) {
                  log.debug("objectGraphRemove(): this node "+ fqn + " is currently referenced by a cyclic reference: "
                          + newFqn + "Will only decrement reference count.");
               }
            } else
            {
               // Relocate all the contents from old to the new fqn
               internal_.relocate(fqn, newFqn);
               // Reset the fqn in the cache interceptor
               InstanceAdvisor advisor = ((Advised) pojo)._getInstanceAdvisor();
               CacheInterceptor interceptor = (CacheInterceptor) AopUtil.findCacheInterceptor(advisor);
               if(interceptor == null)
                  throw new IllegalStateException("ObjectGraphHandler.objectGraphRemove(): null interceptor");
               interceptor.setFqn(newFqn);
               // reset the fqn in the indirect fqn map
               internal_.setIndirectFqn(fqn.toString(), newFqn.toString());

               isTrue = true;

               if (log.isDebugEnabled()) {
                  log.debug("objectGraphRemove(): this node "+ fqn + " is currently referenced by " +
                          +internal_.getRefCount(newFqn) +
                          " other pojos after relocating to " +newFqn.toString());
               }
            }
         }
      }

      return isTrue;
   }

   /**
    * Remove the object from the the reference fqn, meaning just decrement the ref counter.
    * @param fqn
    * @param refFqn
    * @param removeCacheInterceptor
    * @param evict
    * @throws CacheException
    */
   private void removeFromReference(Fqn fqn, String refFqn, boolean removeCacheInterceptor,
                     boolean evict) throws CacheException {
      synchronized (refFqn) {  // we lock the internal fqn here so no one else has access.
         // Decrement ref counting on the internal node
         if (decrementRefCount(Fqn.fromString(refFqn), fqn) == AOPInstance.INITIAL_COUNTER_VALUE) {
            // No one is referring it so it is safe to remove
            // TODO we should make sure the parent nodes are also removed they are empty as well.
            delegate_._removeObject(Fqn.fromString(refFqn), removeCacheInterceptor, evict);
         }
      }

      // Remove ref fqn from this fqn
      internal_.removeRefFqn(fqn);
   }

   /**
    * 1. increment reference counter
    * 2. put in refFqn so we can get it.
    *
    * @param fqn The original fqn node
    * @param refFqn The new internal fqn node
    */
   void setupRefCounting(Fqn fqn, Fqn refFqn) throws CacheException
   {
      synchronized (refFqn) { // we lock the ref fqn here so no one else has access.
         // increment the reference counting
         String aliasFqn = null;
         if( incrementRefCount(refFqn, fqn) == 1 )
         {
            // We have the first multiple reference
            aliasFqn = internal_.createIndirectFqn(refFqn.toString());
         } else
         {
            aliasFqn = internal_.getIndirectFqn(refFqn.toString());
         }
         // set the internal fqn in fqn so we can reference it.
         if(log.isTraceEnabled())
         {
            log.trace("setupRefCounting(): current fqn: " +fqn + " set to point to: " +refFqn);
         }

         internal_.setRefFqn(fqn, aliasFqn);
      }
   }

   int incrementRefCount(Fqn originalFqn, Fqn referencingFqn) throws CacheException
   {
      return internal_.incrementRefCount(originalFqn, referencingFqn);
   }

   int decrementRefCount(Fqn originalFqn, Fqn referencingFqn) throws CacheException
   {
      int count = 0;
      if( (count = internal_.decrementRefCount(originalFqn, referencingFqn)) == (AOPInstance.INITIAL_COUNTER_VALUE +1) )
      {
         internal_.removeIndirectFqn(originalFqn.toString());
      }

      return count;
   }
}
