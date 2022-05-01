/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;


/**
 * An abstract SortedEvictionAlgorithm.
 * <p/>
 * This class supports early termination of the eviction queue processing. Because the eviction
 * queue is sorted by first to evict to last to evict, when iterating the eviction queue, the first time
 * a node is encountered that does not require eviction will terminate the loop early. This way we don't incur
 * the full breadth of the O(n) = n operation everytime we need to check for eviction (defined by eviction poll time
 * interval).
 *
 * @author Daniel Huang - dhuang@jboss.org - 10/2005
 */
public abstract class BaseSortedEvictionAlgorithm extends BaseEvictionAlgorithm implements EvictionAlgorithm
{
   private static final Log log = LogFactory.getLog(BaseSortedEvictionAlgorithm.class);

   public void process(Region region) throws EvictionException
   {
      super.process(region);
   }

   protected void processQueues(Region region) throws EvictionException
   {
      boolean evictionNodesModified = false;

      EvictedEventNode node;
      int count = 0;
      while ((node = region.takeLastEventNode()) != null)
      {
         int eventType = node.getEvent();
         Fqn fqn = node.getFqn();

         count++;
         switch (eventType)
         {
            case EvictedEventNode.ADD_NODE_EVENT:
               this.processAddedNodes(fqn, node.getElementDifference(), node.isResetElementCount());
               evictionNodesModified = true;
               break;
            case EvictedEventNode.REMOVE_NODE_EVENT:
               this.processRemovedNodes(fqn);
               break;
            case EvictedEventNode.VISIT_NODE_EVENT:
               this.processVisitedNodes(fqn);
               evictionNodesModified = true;
               break;
            case EvictedEventNode.ADD_ELEMENT_EVENT:
               this.processAddedElement(fqn);
               evictionNodesModified = true;
               break;
            case EvictedEventNode.REMOVE_ELEMENT_EVENT:
               this.processRemovedElement(fqn);
               evictionNodesModified = true;
               break;
            default:
               throw new RuntimeException("Illegal Eviction Event type " + eventType);
         }
      }

      if (log.isTraceEnabled())
      {
         log.trace("Eviction nodes visited or added requires resort of queue " + evictionNodesModified);
      }

      this.resortEvictionQueue(evictionNodesModified);


      if (log.isTraceEnabled())
      {
         log.trace("processed " + count + " node events");
      }

   }

   /**
    * This method is called to resort the queue after add or visit events have occurred.
    * <p/>
    * If the parameter is true, the queue needs to be resorted. If it is false, the queue does not
    * need resorting.
    *
    * @param evictionQueueModified True if the queue was added to or visisted during event processing.
    */
   protected void resortEvictionQueue(boolean evictionQueueModified)
   {
      if (!evictionQueueModified)
      {
         if (log.isDebugEnabled())
         {
            log.debug("Eviction queue not modified. Resort unnecessary.");
         }
         return;
      }
      long begin = System.currentTimeMillis();
      ((SortedEvictionQueue) evictionQueue).resortEvictionQueue();
      long end = System.currentTimeMillis();

      if (log.isTraceEnabled())
      {
         long diff = end - begin;
         log.trace("Took " + diff + "ms to sort queue with " + getEvictionQueue().getNumberOfNodes() + " elements");
      }
   }

}
