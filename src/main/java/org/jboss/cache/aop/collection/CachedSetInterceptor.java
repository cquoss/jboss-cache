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
 * Set interceptor that delegates underlying impl.
 *
 * @author Ben Wang
 */
public class CachedSetInterceptor extends AbstractCollectionInterceptor
{
//   protected static final Log log_=LogFactory.getLog(CachedSetInterceptor.class);

   protected Map methodMap_;
   protected static final Map managedMethods_ =
         CollectionInterceptorUtil.getManagedMethods(Set.class);
   protected Set cacheImpl_;
   protected Set current_;
   protected Set inMemImpl_;

   public CachedSetInterceptor(PojoCache cache, Fqn fqn, Class clazz, Set obj)
   {
      this.fqn_ = fqn;
      methodMap_ = CollectionInterceptorUtil.getMethodMap(clazz);
      cacheImpl_ = new CachedSetImpl(cache, this);
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
         throw new IllegalStateException("CachedSetInterceptor.toCache(). inMemImpl is null.");

      for(Iterator it = inMemImpl_.iterator(); it.hasNext();)
      {
         Object obj = it.next();
         it.remove();
         cacheImpl_.add(obj);
      }

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
         inMemImpl_ = new HashSet();
      }

      // TODO. This needs optimization.
      inMemImpl_.clear();
      for(Iterator it = cacheImpl_.iterator(); it.hasNext();)
      {
         Object obj = it.next();
         if(removeFromCache)
            it.remove();
         inMemImpl_.add(obj);
      }
   }


   public String getName()
   {
      return "CachedSetInterceptor";
   }

   public Object invoke(org.jboss.aop.joinpoint.Invocation invocation) throws Throwable
   {
      if( current_ == null)
         throw new IllegalStateException("CachedSetInterceptor.invoke(). current_ is null.");

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
