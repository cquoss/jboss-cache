package org.jboss.cache.eviction;

import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;

import java.util.Set;

/**
 * Base class implementation of EvictionPolicy and TreeCacheListener.
 *
 * @author Ben Wang  2-2004
 * @author Daniel Huang - dhuang@jboss.org
 * @version $Revision: 1000 $
 */
public abstract class BaseEvictionPolicy implements EvictionPolicy
{
   protected TreeCache cache_;

   public BaseEvictionPolicy()
   {
   }

   /** EvictionPolicy interface implementation */

   /**
    * Evict the node under given Fqn from cache.
    *
    * @param fqn The fqn of a node in cache.
    * @throws Exception
    */
   public void evict(Fqn fqn) throws Exception
   {
      cache_.evict(fqn);
   }

   /**
    * Return a set of child names under a given Fqn.
    *
    * @param fqn Get child names for given Fqn in cache.
    * @return Set of children name as Objects
    */
   public Set getChildrenNames(Fqn fqn)
   {
      try
      {
         return cache_.getChildrenNames(fqn);
      }
      catch (CacheException e)
      {
         e.printStackTrace();
      }
      return null;
   }

   public boolean hasChild(Fqn fqn)
   {
      return cache_.hasChild(fqn);
   }

   public Object getCacheData(Fqn fqn, Object key)
   {
      try
      {
         return cache_.get(fqn, key);
      }
      catch (CacheException e)
      {
         e.printStackTrace();
      }
      return null;
   }

   public void configure(TreeCache cache)
   {
      this.cache_ = cache;
   }

   /* 
    * (non-Javadoc)
    * @see org.jboss.cache.eviction.EvictionPolicy#canIgnoreEvent(org.jboss.cache.Fqn)
    * 
    */
   public boolean canIgnoreEvent(Fqn fqn) {
       return false;
   }
}
