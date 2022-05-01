/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.aop.Advised;
import org.jboss.aop.InstanceAdvisor;
import org.jboss.aop.advice.Interceptor;
import org.jboss.cache.Fqn;
import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.aop.BaseInterceptor;
import org.jboss.cache.aop.CacheInterceptor;
import org.jboss.cache.aop.PojoCache;

import java.io.Serializable;
import java.util.Set;

/**
 * Unitlity methods for put, get and remove Collection classes object.
 *
 * @author Ben Wang
 */
public class AopUtil
{
   static Log log=LogFactory.getLog(AopUtil.class.getName());

   /**
    * Find cache interceptor with exact fqn.
    * @param advisor
    * @param fqn
    * @return Interceptor
    */
   static public Interceptor findCacheInterceptor(InstanceAdvisor advisor, Fqn fqn)
   {
      org.jboss.aop.advice.Interceptor[] interceptors = advisor.getInterceptors();
      // Step Check for cross references
      for (int i = 0; i < interceptors.length; i++) {
         Interceptor interceptor = interceptors[i];
         if (interceptor instanceof CacheInterceptor) {
            CacheInterceptor inter = (CacheInterceptor)interceptor;
            if (inter != null && inter.getFqn().equals(fqn))
            {
               return interceptor;
            }
         }
      }
      return null;
   }

   /**
    * Find existing cache interceptor. Since there is supposedly only one cache interceptor per
    * pojo, this call should suffice. In addition, in cases of cross or circular reference,
    * fqn can be different anyway.
    * @param advisor
    * @return Interceptor
    */
   static public Interceptor findCacheInterceptor(InstanceAdvisor advisor)
   {
      // TODO we assume there is only one interceptor now.
      Interceptor[] interceptors = advisor.getInterceptors();
      // Step Check for cross references
      for (int i = 0; i < interceptors.length; i++) {
         Interceptor interceptor = interceptors[i];
         if (interceptor instanceof CacheInterceptor) {
               return interceptor;
         }
      }
      return null;
   }

   /**
    * Find existing Collection interceptor. Since there is supposedly only one Collection interceptor per
    * instance, this call should suffice. In addition, in cases of cross or circular reference,
    * fqn can be different anyway.
    * @param advisor
    * @return Interceptor
    */
   static public Interceptor findCollectionInterceptor(InstanceAdvisor advisor)
   {
      // TODO we assume there is only one interceptor now.
      Interceptor[] interceptors = advisor.getInterceptors();
      // Step Check for cross references
      for (int i = 0; i < interceptors.length; i++) {
         Interceptor interceptor = interceptors[i];
         if (interceptor instanceof BaseInterceptor) {
               return interceptor;
         }
      }
      return null;
   }

   /**
    * Check whether the object type is valid. An object type is valid if it is either: aspectized,
    * Serializable, or primitive type. Otherwise a runtime exception is thrown.
    *
    * @param obj
    */
   public static void checkObjectType(Object obj) {
      if(obj == null) return;
      if( ! (obj instanceof Advised) )
      {
          boolean allowedType = (obj instanceof Serializable) || (obj.getClass().isArray() && obj.getClass().getComponentType().isPrimitive());

          if( !allowedType)
          {
               throw new IllegalArgumentException("PojoCache.putObject(): Object type is neither " +
                  " aspectized nor Serializable nor an array of primitives. Object class name is " +obj.getClass().getName());
          }
      }
   }


   public static Fqn constructFqn(Fqn baseFqn, Object relative)
   {
      // TODO Don't know why. But this will fail the CachedSetAopTest clear() method since look up is always
      // Null at the last index. But why?
      // TODO also see JBCACHE-282
      return new Fqn(baseFqn, relative.toString());

//      String tmp = baseFqn.toString() +"/" + relative.toString();
//      return Fqn.fromString(tmp);
//      Fqn fqn = new Fqn((String)relative);
//      return new Fqn(baseFqn, fqn);
   }


   public static DataNode get(PojoCache cache, Fqn fqn) throws CacheException
   {
      if (cache.getCacheLoader() != null)
      {
         // We have cache loader, we can't get it directly from the local get.
         return cache.get(fqn);
      } else
      {
         return cache._get(fqn);
      }
   }

   public static Set getNodeChildren(PojoCache cache, Fqn fqn) throws CacheException
   {
      if (cache.getCacheLoader() != null)
      {
         // We have cache loader, we can't get it directly from the local get.
         return cache.getChildrenNames(fqn);
      } else
      {
         return cache._getChildrenNames(fqn);
      }
   }

}
