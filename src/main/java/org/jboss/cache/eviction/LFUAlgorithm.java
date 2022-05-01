/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Least Frequently Used algorithm for cache eviction.
 * Note that this algorithm is not thread-safe.
 * <p/>
 * This algorithm relies on maxNodes and minNodes to operate correctly.
 * Eviction takes place using Least Frequently Used algorithm. A node A
 * that is used less than a node B is evicted sooner.
 * <p/>
 * The minNodes property defines a threshold for eviction. If minNodes = 100,
 * the LFUAlgorithm will not evict the cache to anything less than 100 elements
 * still left in cache. The maxNodes property defines the maximum number of nodes
 * the cache will accept before eviction. maxNodes = 0 means that this region is
 * unbounded. minNodes = 0 means that the eviction queue will attempt to bring
 * the cache of this region to 0 elements (evict all elements) whenever it is run.
 * <p/>
 * This algorithm uses a sorted eviction queue. The eviction queue is sorted in
 * ascending order based on the number of times a node is visited. The more frequently
 * a node is visited, the less likely it will be evicted.
 *
 * @author Daniel Huang - dhuang@jboss.org 10/2005
 * @version $Revision: 1852 $
 */
public class LFUAlgorithm extends BaseSortedEvictionAlgorithm implements EvictionAlgorithm
{
   private static final Log log = LogFactory.getLog(LFUAlgorithm.class);


   public LFUAlgorithm()
   {
      super();
   }

   protected boolean shouldEvictNode(NodeEntry ne)
   {
      if (log.isTraceEnabled())
      {
         log.trace("Deciding whether node in queue " + ne.getFqn() + " requires eviction.");
      }

      LFUConfiguration config = (LFUConfiguration) region.getEvictionConfiguration();
      int size = this.getEvictionQueue().getNumberOfNodes();
      if (config.getMaxNodes() != 0 && size > config.getMaxNodes())
      {
         return true;
      }
      else if (size > config.getMinNodes())
      {
         return true;
      }

      return false;
   }

   /**
    * Will create a LFUQueue to be used as the underlying eviction queue.
    *
    * @param region Region to create the eviction queue for.
    * @return The created LFUQueue.
    * @throws EvictionException
    */
   protected EvictionQueue setupEvictionQueue(Region region) throws EvictionException
   {
      return new LFUQueue();
   }

   protected void prune() throws EvictionException
   {
      super.prune();

      // clean up the Queue's eviction removals
      ((LFUQueue) this.evictionQueue).prune();
   }
}
