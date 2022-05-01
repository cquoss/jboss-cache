/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop.collection;

import org.jboss.cache.Fqn;
import org.jboss.cache.aop.BaseInterceptor;
import org.jboss.cache.aop.AOPInstance;

/**
 * Abstract base class for collection interceptor.
 *
 * @author Ben Wang
 * @version $Id: AbstractCollectionInterceptor.java 2949 2006-11-16 06:44:48Z bwang $
 */
public abstract class AbstractCollectionInterceptor implements BaseInterceptor {
   protected Fqn fqn_;
   protected boolean attached_ = true;
   protected AOPInstance aopInstance_;

   public Fqn getFqn()
   {
      return fqn_;
   }

   public void setFqn(Fqn fqn)
   {
      this.fqn_ = fqn;
   }

   public AOPInstance getAopInstance() {
      return aopInstance_;
   }

   public void setAopInstance(AOPInstance aopInstance) {
      this.aopInstance_ = aopInstance;
   }

   public void attach(Fqn fqn, boolean copyToCache)
   {
      // This is a hook to allow re-attching the Collection without specifying the fqn.
      if(fqn != null)
      {
         setFqn(fqn);
      }
      attached_ = true;
      // Reattach anything in-memory to cache
   }

   public void detach(boolean removeFromCache)
   {
      attached_ = false;
      // Detach by tranferring the cache content to in-memory copy
   }

   public boolean isAttached()
   {
      return attached_;
   }

   public Object getOriginalInstance()
   {
      throw new RuntimeException("AbstractCollectionInterceptor.getOriginalInstance(). Abstract method");
   }
}
