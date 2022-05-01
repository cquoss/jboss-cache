/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import EDU.oswego.cs.dl.util.concurrent.BoundedBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.lock.TimeoutException;

/**
 * Abstract Event Processing Eviction Algorithm.
 * This class is used to implement basic event processing for Eviction Algorithms.
 * To extend this abstract class to make an Eviction Algorithm, implement the
 * abstract methods and a policy.
 *
 * @author Daniel Huang - dhuang@jboss.org 10/2005
 * @version $Revision: 3157 $
 */
public abstract class BaseEvictionAlgorithm implements EvictionAlgorithm
{
   private static final Log log = LogFactory.getLog(BaseEvictionAlgorithm.class);

   /**
    * Mapped region.
    */
   protected Region region;

   /**
    * Contains Fqn instances.
    */
   protected BoundedBuffer recycleQueue;

   /**
    * Contains NodeEntry instances.
    */
   protected EvictionQueue evictionQueue;

   /**
    * This method will create an EvictionQueue implementation and prepare it for use.
    *
    * @param region Region to setup an eviction queue for.
    * @return The created EvictionQueue to be used as the eviction queue for this algorithm.
    * @throws EvictionException
    * @see EvictionQueue
    */
   protected abstract EvictionQueue setupEvictionQueue(Region region) throws EvictionException;

   /**
    * This method will check whether the given node should be evicted or not.
    *
    * @param ne NodeEntry to test eviction for.
    * @return True if the given node should be evicted. False if the given node should not be evicted.
    */
   protected abstract boolean shouldEvictNode(NodeEntry ne);

   protected BaseEvictionAlgorithm()
   {
      recycleQueue = new BoundedBuffer(RegionManager.CAPACITY);
   }

   protected void initialize(Region region) throws EvictionException
   {
      this.region = region;
      evictionQueue = setupEvictionQueue(region);
   }

   /**
    * Process the given region.
    * <p/>
    * Eviction Processing encompasses the following:
    * <p/>
    * - Add/Remove/Visit Nodes
    * - Prune according to Eviction Algorithm
    * - Empty/Retry the recycle queue of previously evicted but locked (during actual cache eviction) nodes.
    *
    * @param region Cache region to process for eviction.
    * @throws EvictionException
    */
   public void process(Region region) throws EvictionException
   {
      if (this.region == null)
      {
         this.initialize(region);
      }

      if (log.isTraceEnabled())
      {
         log.trace("process(): region: " + region.getFqn());
      }

      this.processQueues(region);
      this.emptyRecycleQueue();
      this.prune();
   }

   public void resetEvictionQueue(Region region)
   {
   }

   /**
    * Get the underlying EvictionQueue implementation.
    *
    * @return the EvictionQueue used by this algorithm
    * @see EvictionQueue
    */
   public EvictionQueue getEvictionQueue()
   {
      return this.evictionQueue;
   }

   /**
    * Event processing for Evict/Add/Visiting of nodes.
    * <p/>
    * - On AddEvents a new element is added into the eviction queue
    * - On RemoveEvents, the removed element is removed from the eviction queue.
    * - On VisitEvents, the visited node has its eviction statistics updated (idleTime, numberOfNodeVisists, etc..)
    *
    * @param region Cache region to process for eviction.
    * @throws EvictionException
    */
   protected void processQueues(Region region) throws EvictionException
   {
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
               this.processAddedNodes(fqn,
                     node.getElementDifference(),
                     node.isResetElementCount());
               break;
            case EvictedEventNode.REMOVE_NODE_EVENT:
               this.processRemovedNodes(fqn);
               break;
            case EvictedEventNode.VISIT_NODE_EVENT:
               this.processVisitedNodes(fqn);
               break;
            case EvictedEventNode.ADD_ELEMENT_EVENT:
               this.processAddedElement(fqn);
               break;
            case EvictedEventNode.REMOVE_ELEMENT_EVENT:
               this.processRemovedElement(fqn);
               break;
            case EvictedEventNode.MARK_IN_USE_EVENT:
               this.processMarkInUseNodes(fqn, node.getInUseTimeout());
               break;
            case EvictedEventNode.UNMARK_USE_EVENT:
               this.processUnmarkInUseNodes(fqn);
               break;
            default:
               throw new RuntimeException("Illegal Eviction Event type " + eventType);
         }
      }

      if (log.isTraceEnabled())
      {
         log.trace("processed " + count + " node events in region: " + region.getFqn());
      }

   }

   protected void evict(NodeEntry ne)
   {
//      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne != null)
      {
         evictionQueue.removeNodeEntry(ne);
         if (!this.evictCacheNode(ne.getFqn()))
         {
            try
            {
               recycleQueue.put(ne.getFqn());
            }
            catch (InterruptedException e)
            {
               log.debug("InterruptedException", e);
            }
         }
      }
   }

   /**
    * Evict a node from cache.
    *
    * @param fqn node corresponds to this fqn
    * @return True if successful
    */
   protected boolean evictCacheNode(Fqn fqn)
   {
      if (log.isTraceEnabled())
      {
         log.trace("Attempting to evict cache node with fqn of " + fqn);
      }
      EvictionPolicy policy = region.getEvictionPolicy();
      // Do an eviction of this node

      try
      {
         policy.evict(fqn);
      }
      catch (Exception e)
      {
         if (e instanceof TimeoutException)
         {
            log.warn("eviction of " + fqn + " timed out. Will retry later.");
            return false;
         }
         e.printStackTrace();
         return false;
      }

      if (log.isTraceEnabled())
      {
         log.trace("Eviction of cache node with fqn of " + fqn + " successful");
      }

      return true;
   }

   protected void processMarkInUseNodes(Fqn fqn, long inUseTimeout) throws EvictionException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Marking node " + fqn + " as in use with a usage timeout of " + inUseTimeout);
      }

      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne != null)
      {
         ne.setCurrentlyInUse(true, inUseTimeout);
      }
   }

   protected void processUnmarkInUseNodes(Fqn fqn) throws EvictionException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Unmarking node " + fqn + " as in use");
      }

      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne != null)
      {
         ne.setCurrentlyInUse(false, 0);
      }
   }

   protected void processAddedNodes(Fqn fqn, int numAddedElements, boolean resetElementCount) throws EvictionException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Adding node " + fqn + " with " + numAddedElements + " elements to eviction queue");
      }

      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne != null)
      {
         ne.setModifiedTimeStamp(System.currentTimeMillis());
         ne.setNumberOfNodeVisits(ne.getNumberOfNodeVisits() + 1);
         if (resetElementCount)
         {
            ne.setNumberOfElements(numAddedElements);
         }
         else
         {
            ne.setNumberOfElements(ne.getNumberOfElements() + numAddedElements);
         }
         return;
      }

      long stamp = System.currentTimeMillis();
      ne = new NodeEntry(fqn);
      ne.setModifiedTimeStamp(stamp);
      ne.setNumberOfNodeVisits(1);
      ne.setNumberOfElements(numAddedElements);
      // add it to the node map and eviction queue
      if (evictionQueue.containsNodeEntry(ne))
      {
         if (log.isTraceEnabled())
         {
            log.trace("Queue already contains " + ne.getFqn() + " processing it as visited");
         }
         this.processVisitedNodes(ne.getFqn());
         return;
      }

      evictionQueue.addNodeEntry(ne);

      if (log.isTraceEnabled())
      {
         log.trace(ne.getFqn() + " added successfully to eviction queue");
      }
   }

   /**
    * Remove a node from cache.
    * <p/>
    * This method will remove the node from the eviction queue as well as
    * evict the node from cache.
    * <p/>
    * If a node cannot be removed from cache, this method will remove it from the eviction queue
    * and place the element into the recycleQueue. Each node in the recycle queue will get retried until
    * proper cache eviction has taken place.
    * <p/>
    * Because EvictionQueues are collections, when iterating them from an iterator, use iterator.remove()
    * to avoid ConcurrentModificationExceptions. Use the boolean parameter to indicate the calling context.
    *
    * @param fqn FQN of the removed node
    * @throws EvictionException
    */
   protected void processRemovedNodes(Fqn fqn) throws EvictionException
   {
      if (log.isTraceEnabled())
      {
         log.trace("Removing node " + fqn + " from eviction queue and attempting eviction");
      }

      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne != null)
      {
         evictionQueue.removeNodeEntry(ne);
      }
      else
      {
         if (log.isDebugEnabled())
         {
            log.debug("processRemoveNodes(): Can't find node associated with fqn: " + fqn
                  + "Could have been evicted earlier. Will just continue.");
         }
         return;
      }

      if (log.isTraceEnabled())
      {
         log.trace(fqn + " removed from eviction queue");
      }
   }

   /**
    * Visit a node in cache.
    * <p/>
    * This method will update the numVisits and modifiedTimestamp properties of the Node.
    * These properties are used as statistics to determine eviction (LRU, LFU, MRU, etc..)
    * <p/>
    * *Note* that this method updates Node Entries by reference and does not put them back
    * into the queue. For some sorted collections, a remove, and a re-add is required to
    * maintain the sorted order of the elements.
    *
    * @param fqn FQN of the visited node.
    * @throws EvictionException
    */
   protected void processVisitedNodes(Fqn fqn) throws EvictionException
   {
      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne == null)
      {
         if (log.isDebugEnabled())
         {
            log.debug("Visiting node that was not added to eviction queues. Assuming that it has 1 element.");
         }
         this.processAddedNodes(fqn, 1, false);
         return;
      }
      // note this method will visit and modify the node statistics by reference!
      // if a collection is only guaranteed sort order by adding to the collection,
      // this implementation will not guarantee sort order.
      ne.setNumberOfNodeVisits(ne.getNumberOfNodeVisits() + 1);
      ne.setModifiedTimeStamp(System.currentTimeMillis());
   }

   protected void processRemovedElement(Fqn fqn) throws EvictionException
   {
      NodeEntry ne = evictionQueue.getNodeEntry(fqn);

      if (ne == null)
      {
         if (log.isDebugEnabled())
         {
            log.debug("Removing element from " + fqn + " but eviction queue does not contain this node. " +
                  "Ignoring removeElement event.");
         }
         return;
      }

      ne.setNumberOfElements(ne.getNumberOfElements() - 1);
      // also treat it as a node visit.
      ne.setNumberOfNodeVisits(ne.getNumberOfNodeVisits() + 1);
      ne.setModifiedTimeStamp(System.currentTimeMillis());
   }

   protected void processAddedElement(Fqn fqn) throws EvictionException
   {
      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne == null)
      {
         if (log.isDebugEnabled())
         {
            log.debug("Adding element " + fqn + " for a node that doesn't exist yet. Process as an add.");
         }
         this.processAddedNodes(fqn, 1, false);
         return;
      }

      ne.setNumberOfElements(ne.getNumberOfElements() + 1);

      // also treat it as a node visit.
      ne.setNumberOfNodeVisits(ne.getNumberOfNodeVisits() + 1);
      ne.setModifiedTimeStamp(System.currentTimeMillis());
   }


   /**
    * Empty the Recycle Queue.
    * <p/>
    * This method will go through the recycle queue and retry to evict the nodes from cache.
    *
    * @throws EvictionException
    */
   protected void emptyRecycleQueue() throws EvictionException
   {
      while (true)
      {
         Fqn fqn;

         try
         {
            fqn = (Fqn) recycleQueue.poll(0);
         }
         catch (InterruptedException e)
         {
            e.printStackTrace();
            break;
         }

         if (fqn == null)
         {
            if (log.isTraceEnabled())
            {
               log.trace("Recycle queue is empty");
            }
            break;
         }

         if (log.isTraceEnabled())
         {
            log.trace("emptying recycle bin. Evict node " + fqn);
         }

         // Still doesn't work
         if (!evictCacheNode(fqn))
         {
            try
            {
               recycleQueue.put(fqn);
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
            break;
         }
      }
   }

   protected boolean isNodeInUseAndNotTimedOut(NodeEntry ne)
   {
      if (ne.isCurrentlyInUse())
      {
         if (ne.getInUseTimeoutTimestamp() == 0)
         {
            return true;
         }

         if (System.currentTimeMillis() < ne.getInUseTimeoutTimestamp())
         {
            return true;
         }
      }
      return false;
   }


   protected void prune() throws EvictionException
   {
      NodeEntry entry;
      while ((entry = evictionQueue.getFirstNodeEntry()) != null)
      {
         if (this.shouldEvictNode(entry))
         {
            this.evict(entry);
         }
         else
         {
            break;
         }
      }
   }

}
