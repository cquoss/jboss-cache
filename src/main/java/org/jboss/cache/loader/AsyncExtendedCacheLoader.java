/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.loader;

import org.jboss.cache.Fqn;
import org.jboss.cache.marshall.RegionManager;

/**
 * Extends AsyncCacheLoader to supply additional ExtendedCacheLoader methods.
 * 
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public class AsyncExtendedCacheLoader extends AsyncCacheLoader implements ExtendedCacheLoader
{
   private ExtendedCacheLoader delegateTo;
   
   /**
    * Create a new AsyncExtendedCacheLoader.
    * 
    * @param cacheLoader
    */
   public AsyncExtendedCacheLoader(ExtendedCacheLoader cacheLoader)
   {
      super(cacheLoader);
      this.delegateTo = cacheLoader;
   }

   public byte[] loadState(Fqn subtree) throws Exception
   {
      return delegateTo.loadState(subtree);
   }

   public void storeState(byte[] state, Fqn subtree) throws Exception
   {
      super.storeState(state, subtree);
   }

   public void setRegionManager(RegionManager manager)
   {
      delegateTo.setRegionManager(manager);
   }

}
