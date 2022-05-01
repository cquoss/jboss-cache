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
 * First-in-first-out algorithm used to evict nodes.
 *
 * @author Daniel Huang - dhuang@jboss.org
 * @author Morten Kvistgaard
 * @version $Revision: 1852 $
 */
public class FIFOAlgorithm extends BaseEvictionAlgorithm
{
   private static final Log log = LogFactory.getLog(FIFOAlgorithm.class);


   public FIFOAlgorithm()
   {
      super();
   }

   protected EvictionQueue setupEvictionQueue(Region region) throws EvictionException
   {
      return new FIFOQueue();
   }

   /**
    * For FIFO, a node should be evicted if the queue size is >= to the configured maxNodes size.
    */
   protected boolean shouldEvictNode(NodeEntry ne)
   {
      FIFOConfiguration config = (FIFOConfiguration) region.getEvictionConfiguration();
      if (log.isTraceEnabled())
      {
         log.trace("Deciding whether node in queue " + ne.getFqn() + " requires eviction.");
      }

      int size = this.getEvictionQueue().getNumberOfNodes();
      return config.getMaxNodes() != 0 && size > config.getMaxNodes();

   }

}

