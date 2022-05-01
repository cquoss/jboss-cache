/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.CacheException;
import org.jboss.cache.aop.collection.CollectionInterceptorUtil;
import org.jboss.cache.aop.collection.AbstractCollectionInterceptor;
import org.jboss.aop.proxy.ClassProxy;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.InstanceAdvisor;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Handling the Collection class management.
 *
 * @author Ben Wang
 *         Date: Aug 4, 2005
 * @version $Id: CollectionClassHandler.java 2569 2006-09-17 08:25:35Z bwang $
 */
public class CollectionClassHandler {
   protected final static Log log=LogFactory.getLog(CollectionClassHandler.class);
   protected PojoCache cache_;
   protected InternalDelegate internal_;
   protected ObjectGraphHandler graphHandler_;

   public CollectionClassHandler(PojoCache cache, InternalDelegate internal, ObjectGraphHandler graphHandler)
   {
      cache_ = cache;
      internal_ = internal;
      graphHandler_ = graphHandler;
   }

   Object collectionObjectGet(Fqn fqn, Class clazz)
           throws CacheException
   {
      Object obj = null;
      try {
         if(Map.class.isAssignableFrom(clazz)) {
            Object map = clazz.newInstance();
            obj=CollectionInterceptorUtil.createMapProxy(cache_, fqn, clazz, (Map)map);
         } else if(List.class.isAssignableFrom(clazz)) {
            Object list = clazz.newInstance();
            obj=CollectionInterceptorUtil.createListProxy(cache_, fqn, clazz, (List)list);
         } else if(Set.class.isAssignableFrom(clazz)) {
            Object set = clazz.newInstance();
            obj=CollectionInterceptorUtil.createSetProxy(cache_, fqn, clazz, (Set)set);
         }
      } catch (Exception e) {
         throw new CacheException("failure creating proxy", e);
      }

      return obj;
   }


   boolean collectionObjectPut(Fqn fqn, Object obj) throws CacheException {
      boolean isCollection = false;

      CachedType type = null;
      AbstractCollectionInterceptor interceptor = null;
      if(obj instanceof ClassProxy)
      {
         Class originalClaz = obj.getClass().getSuperclass();
         interceptor = CollectionInterceptorUtil.getInterceptor((ClassProxy)obj);
         type = cache_.getCachedType(originalClaz);
      } else
      {
         type = cache_.getCachedType(obj.getClass());
      }

      if(obj instanceof ClassProxy)
      {
         // A proxy here. We may have multiple references.
         if(interceptor == null)
         {
            if(log.isDebugEnabled())
            {
               log.debug("collectionObjectPut(): null interceptor. Could be removed previously. "+fqn);
            }
         } else
         {
            if( interceptor.isAttached() ) // If it is not attached, it is not active.
            {
               // Let's check for object graph, e.g., multiple and circular references first
               if (graphHandler_.objectGraphPut(fqn, interceptor, type, obj)) { // found cross references
                  return true;
               }
            } else
            {
               // Re-attach the interceptor to this fqn.
               boolean copyToCache = true;
               interceptor.attach(fqn, copyToCache);
               internal_.putAopClazz(fqn, type.getType());
               internal_.setPojo(fqn, obj);
               return true; // we are done
            }
         }
      }

      //JBCACHE-760: for collection - put initialized aopInstance in fqn
      if (!(obj instanceof Map || obj instanceof List || obj instanceof Set)) {
          return false;
      }

      // Always initialize the ref count so that we can mark this as an AopNode.
      AOPInstance aopInstance = internal_.initializeAopInstance(fqn);
      cache_.put(fqn, AOPInstance.KEY, aopInstance);

      if (obj instanceof Map) {
         if (log.isDebugEnabled()) {
            log.debug("collectionPutObject(): aspectized obj is a Map type of size: " + ((Map) obj).size());
         }

         internal_.putAopClazz(fqn, type.getType());

         // Let's replace it with a proxy if necessary
         Map map = (Map)obj;
         if( !(obj instanceof ClassProxy)) {
            Class clazz = obj.getClass();
            try {
               obj=CollectionInterceptorUtil.createMapProxy(cache_, fqn, clazz, (Map)obj);
            } catch (Exception e) {
               throw new CacheException("failure creating proxy", e);
            }
         }

         isCollection = true;
         // populate via the proxied collection
         for (Iterator i = map.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Map.Entry) i.next();
            ((Map)obj).put(entry.getKey(), entry.getValue());
         }

      } else if (obj instanceof List) {
         if (log.isDebugEnabled()) {
            log.debug("collectionPutObject(): aspectized obj is a List type of size: "
                  + ((List) obj).size());
         }

         List list = (List) obj;
         internal_.putAopClazz(fqn, type.getType());

         // Let's replace it with a proxy if necessary
         if( !(obj instanceof ClassProxy)) {
            Class clazz = obj.getClass();
            try {
               obj=CollectionInterceptorUtil.createListProxy(cache_, fqn, clazz, (List)obj);
            } catch (Exception e) {
               throw new CacheException("failure creating proxy", e);
            }
         }

         isCollection = true;
         // populate via the proxied collection
         for (Iterator i = list.iterator(); i.hasNext();) {
            ((List)obj).add(i.next());
         }

      } else if (obj instanceof Set) {
         if (log.isDebugEnabled()) {
            log.debug("collectionPutObject(): aspectized obj is a Set type of size: "
                  + ((Set) obj).size());
         }

         Set set = (Set) obj;
         internal_.putAopClazz(fqn, type.getType());

         // Let's replace it with a proxy if necessary
         if( !(obj instanceof ClassProxy)) {
            Class clazz = obj.getClass();
            try {
               obj=CollectionInterceptorUtil.createSetProxy(cache_, fqn, clazz, (Set)obj);
            } catch (Exception e) {
               throw new CacheException("failure creating proxy", e);
            }
         }

         isCollection = true;
         // populate via the proxied collection
         for (Iterator i = set.iterator(); i.hasNext();) {
            ((Set)obj).add(i.next());
         }

      }

      if(isCollection)
      {
         // Attach aopInstance to that interceptor
         BaseInterceptor baseInterceptor = (BaseInterceptor)CollectionInterceptorUtil.getInterceptor((ClassProxy)obj);
         baseInterceptor.setAopInstance(aopInstance);

         internal_.setPojo(aopInstance, obj);
      }
      return isCollection;
   }

   boolean collectionObjectRemove(Fqn fqn, boolean removeCacheInterceptor,
                                  boolean evict) throws CacheException
   {
      Class clazz = internal_.peekAopClazz(fqn);

      if (!Map.class.isAssignableFrom(clazz) && !Collection.class.isAssignableFrom(clazz))
      {
         return false;
      }

      Object obj = cache_.getObject(fqn);
      if( !(obj instanceof ClassProxy))
      {
         throw new RuntimeException("CollectionClassHandler.collectionRemoveObject(): object is not a proxy :" +obj);
      }

      Interceptor interceptor = CollectionInterceptorUtil.getInterceptor((ClassProxy)obj);
      boolean removeFromCache = true;
      ((AbstractCollectionInterceptor)interceptor).detach(removeFromCache); // detach the interceptor. This will trigger a copy and remove.

      return true;
   }

   void collectionReplaceWithProxy(Object obj, Object value, Field field, Fqn tmpFqn)
           throws CacheException
   {
      // If value (field member) is of Collection type, e.g., composite class
      // that contains Collection member, we will swap out the old reference
      // with the proxy one.
      // This can probably be optimized with check for instanceof proxy
      if( value instanceof Map || value instanceof List || value instanceof Set ) {
         Object newValue = cache_.getObject(tmpFqn);
         try {
            field.set(obj, newValue);
            cache_.addUndoCollectionProxy(field, obj, value);
         } catch (IllegalAccessException e) {
            log.error("collectionReplaceWithProxy(): Can't swap out the Collection class of field " +field.getName() +
                  "Exception " +e);
            throw new CacheException("CollectionClassHandler.collectionReplaceWithProxy(): Can't swap out the Collection class of field \" +field.getName(),"
                  +e);
         }
      }
   }
}
