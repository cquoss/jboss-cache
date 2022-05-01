/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.collection;

import org.jboss.cache.Fqn;
import org.jboss.cache.aop.PojoCache;

import java.util.*;

/**
 * Map interceptor that delegates to the underlying impl.
 *
 * @author Ben Wang
 */
public class CachedMapInterceptor extends AbstractCollectionInterceptor
{

//   protected static final Log log_ = LogFactory.getLog(CachedMapInterceptor.class);
   protected static final Map managedMethods_ =
         CollectionInterceptorUtil.getManagedMethods(Map.class);
   protected Map methodMap_;
   protected Map cacheImpl_;
   protected Map inMemImpl_;
   protected Map current_;

   protected CachedMapInterceptor(PojoCache cache, Fqn fqn, Class clazz, Map obj)
   {
      this.fqn_ = fqn;
      methodMap_ = CollectionInterceptorUtil.getMethodMap(clazz);
      cacheImpl_ = new CachedMapImpl(cache, this);
      inMemImpl_ = obj;
      current_ = cacheImpl_;
   }

   /**
    * When we want to associate this proxy with the cache again. We will need to translate the in-memory
    * content to the cache store first.
    */
   public void attach(Fqn fqn, boolean copyToCache)
   {
      super.attach(fqn, copyToCache);

      if(copyToCache)
         toCache();

      current_ = cacheImpl_;
   }

   protected void toCache()
   {
      if(inMemImpl_ == null)
         throw new IllegalStateException("CachedMapInterceptor.toCache(). inMemImpl is null.");

      Iterator it = inMemImpl_.keySet().iterator();
      while(it.hasNext())
      {
         Object key = it.next();
         Object val = inMemImpl_.get(key);
         cacheImpl_.put(key, val);
      }

      inMemImpl_.clear();
//      inMemImpl_ = null;   // we are done with this.
   }

   /**
    * When we want to separate this proxy from the cache. We will destroy the cache content and copy them to
    * the in-memory copy.
    */
   public void detach(boolean removeFromCache)
   {
      super.detach(removeFromCache);

      toMemory(removeFromCache);

      current_ = inMemImpl_;
   }

   protected void toMemory(boolean removeFromCache)
   {
      if(inMemImpl_ == null)
      {
         inMemImpl_ = new HashMap();
      }

      Iterator it = cacheImpl_.keySet().iterator();
      inMemImpl_.clear();
      while(it.hasNext())
      {
         Object key = it.next();
         Object val = null;
         if(removeFromCache)
         {
            val = cacheImpl_.remove(key);
         } else
         {
            val = cacheImpl_.get(key);
         }
         inMemImpl_.put(key, val);
      }
   }

   public String getName()
   {
      return "CachedMapInterceptor";
   }

   public Object invoke(org.jboss.aop.joinpoint.Invocation invocation) throws Throwable
   {
      if( current_ == null)
         throw new IllegalStateException("CachedMapInterceptor.invoke(). current_ is null.");

      return CollectionInterceptorUtil.invoke(invocation,
            current_,
            methodMap_,
            managedMethods_);
   }

   public Object getOriginalInstance()
   {
      return inMemImpl_;
   }

}
