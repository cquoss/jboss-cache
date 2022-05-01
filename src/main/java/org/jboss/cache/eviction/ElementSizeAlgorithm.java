/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;


/**
 * @author Daniel Huang
 * @version $Revision: 1852 $
 */
public class ElementSizeAlgorithm extends BaseSortedEvictionAlgorithm
{
   protected EvictionQueue setupEvictionQueue(Region region) throws EvictionException
   {
      return new ElementSizeQueue();
   }

   protected boolean shouldEvictNode(NodeEntry ne)
   {
      ElementSizeConfiguration config = (ElementSizeConfiguration) region.getEvictionConfiguration();

      int size = this.getEvictionQueue().getNumberOfNodes();
      if (config.getMaxNodes() != 0 && size > config.getMaxNodes())
      {
         return true;
      }

      return ne.getNumberOfElements() > config.getMaxElementsPerNode();
   }


   protected void prune() throws EvictionException
   {
      super.prune();

      // clean up the Queue's eviction removals
      ((ElementSizeQueue) this.evictionQueue).prune();
   }

}
