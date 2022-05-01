/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.jboss.cache.Fqn;

/**
 * Most Recently Used Algorithm.
 * <p/>
 * This algorithm will evict the most recently used cache entries from cache.
 * <p/>
 * Note: None of the Eviction classes are thread safe. It is assumed that an individual instance of an EvictionPolicy/
 * EvictionAlgorithm/EvictionQueue/EvictionConfiguration are only operated on by one thread at any given time.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 1592 $
 */
public class MRUAlgorithm extends BaseEvictionAlgorithm
{
   protected EvictionQueue setupEvictionQueue(Region region) throws EvictionException
   {
      return new MRUQueue();
   }

   protected boolean shouldEvictNode(NodeEntry ne)
   {
      MRUConfiguration config = (MRUConfiguration) region.getEvictionConfiguration();
      return evictionQueue.getNumberOfNodes() > config.getMaxNodes();
   }

   protected void processVisitedNodes(Fqn fqn) throws EvictionException
   {
      super.processVisitedNodes(fqn);
      ((MRUQueue) evictionQueue).moveToTopOfStack(fqn);
   }
}
