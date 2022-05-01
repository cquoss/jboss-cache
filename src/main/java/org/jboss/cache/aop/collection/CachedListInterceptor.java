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
 * List ineterceptor that delegates to underlying implementation.
 *
 * @author Ben Wang
 */

public class CachedListInterceptor extends AbstractCollectionInterceptor
{

//   protected static final Log log_ = LogFactory.getLog(CachedListInterceptor.class);
   protected static final Map managedMethods_ =
         CollectionInterceptorUtil.getManagedMethods(List.class);

   protected Map methodMap_;
   // This is the copy in cache store when it is attached.
   protected List cacheImpl_;
   // This is the copy in-memory when the state is detached.
   protected List inMemImpl_;
   // Whichever is used now.
   protected List current_;

   public CachedListInterceptor(PojoCache cache, Fqn fqn, Class clazz, List obj)
   {
      this.fqn_ = fqn;
      methodMap_ = CollectionInterceptorUtil.getMethodMap(clazz);
      cacheImpl_ = new CachedListImpl(cache, this);
      inMemImpl_ = obj;   // lazy initialization here.
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
         throw new IllegalStateException("CachedListInterceptor.toCache(). inMemImpl is null.");

      // TODO This may not be optimal
      List tmpList = new ArrayList();
      for(int i = inMemImpl_.size(); i > 0;  i--)
      {
         Object obj = inMemImpl_.remove(i-1);
         tmpList.add(obj);
      }

      int size = tmpList.size();
      for(int i=0; i < tmpList.size(); i++)
      {
         cacheImpl_.add(tmpList.get(size-i-1));
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
         inMemImpl_ = new ArrayList();
      }

      // Optimization since remove from the beginning is very expensive.
      List tmpList = new ArrayList();
      for(int i = cacheImpl_.size(); i > 0; i--)
      {
         int j = i-1;
         Object obj = null;
         if(removeFromCache)
         {
            obj = cacheImpl_.remove(j);
         } else
         {
            obj = cacheImpl_.get(j);
         }

         tmpList.add(obj);
      }

      int size = tmpList.size();
      inMemImpl_.clear();
      for(int i=0; i < tmpList.size(); i++)
      {
         inMemImpl_.add(tmpList.get(size-i-1));
      }
   }

   public String getName()
   {
      return "CachedListInterceptor";
   }

   public Object invoke(org.jboss.aop.joinpoint.Invocation invocation) throws Throwable
   {
      if( current_ == null)
         throw new IllegalStateException("CachedListInterceptor.invoke(). current_ is null.");

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
