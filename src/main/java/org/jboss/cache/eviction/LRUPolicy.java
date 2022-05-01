/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.eviction;

import org.jboss.cache.TreeCache;

/**
 * Provider to provide eviction policy. This one is based on LRU algorithm that a user
 * can specify either maximum number of nodes or the idle time of a node to be evicted.
 *
 * @author Ben Wang 02-2004
 * @author Daniel Huang - dhuang@jboss.org
 * @version $Revision: 1542 $
 */
public class LRUPolicy extends BaseEvictionPolicy implements EvictionPolicy
{
   protected RegionManager regionManager_;

   protected EvictionAlgorithm algorithm;

   public LRUPolicy()
   {
      super();
      algorithm = new LRUAlgorithm();
   }

   public EvictionAlgorithm getEvictionAlgorithm()
   {
      return algorithm;
   }

   public Class getEvictionConfigurationClass()
   {
      return LRUConfiguration.class;
   }

   public void configure(TreeCache cache)
   {
      super.configure(cache);
      regionManager_ = cache_.getEvictionRegionManager();
   }
}
