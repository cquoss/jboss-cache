/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.eviction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;

/**
 * Least recently Used algorithm to purge old data.
 * Note that this algorithm is not thread-safe.
 *
 * @author Ben Wang 02-2004
 * @author Daniel Huang - dhuang@jboss.org
 */
public class LRUAlgorithm extends BaseEvictionAlgorithm implements EvictionAlgorithm
{
   private static final Log log = LogFactory.getLog(LRUAlgorithm.class);

   public LRUAlgorithm()
   {
      super();
   }

   protected EvictionQueue setupEvictionQueue(Region region) throws EvictionException
   {
      return new LRUQueue();
   }

   protected boolean shouldEvictNode(NodeEntry entry)
   {
      LRUConfiguration config = (LRUConfiguration) region.getEvictionConfiguration();
      // no idle or max time limit
      if (config.getTimeToLiveSeconds() == 0 && config.getMaxAgeSeconds() == 0) return false;

      long currentTime = System.currentTimeMillis();
      if (config.getTimeToLiveSeconds() != 0)
      {
         long idleTime = currentTime - entry.getModifiedTimeStamp();
         if (log.isTraceEnabled())
         {
            log.trace("Node " + entry.getFqn() + " has been idle for " + idleTime + "ms");
         }
         if ((idleTime >= (config.getTimeToLiveSeconds() * 1000)))
         {
            if (log.isTraceEnabled())
            {
               log.trace("Node " + entry.getFqn() + " should be evicted because of idle time");
            }
            return true;
         }
      }

      if (config.getMaxAgeSeconds() != 0)
      {
         long objectLifeTime = currentTime - entry.getCreationTimeStamp();
         if (log.isTraceEnabled())
         {
            log.trace("Node " + entry.getFqn() + " has been alive for " + objectLifeTime + "ms");
         }
         if ((objectLifeTime >= (config.getMaxAgeSeconds() * 1000)))
         {
            if (log.isTraceEnabled())
            {
               log.trace("Node " + entry.getFqn() + " should be evicted because of max age");
            }
            return true;
         }
      }

      if (log.isTraceEnabled())
      {
         log.trace("Node " + entry.getFqn() + " should not be evicted");
      }
      return false;
   }

   protected void evict(NodeEntry ne)
   {
//      NodeEntry ne = evictionQueue.getNodeEntry(fqn);
      if (ne != null)
      {
//         evictionQueue.removeNodeEntry(ne);
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

   protected void prune() throws EvictionException
   {
      LRUQueue lruQueue = (LRUQueue) evictionQueue;
      NodeEntry ne;
      Iterator it = lruQueue.iterateLRUQueue();
      while (it.hasNext())
      {
         ne = (NodeEntry) it.next();
         if (isNodeInUseAndNotTimedOut(ne))
         {
            continue;
         }

         if (this.shouldEvictNode(ne))
         {
            it.remove();
            lruQueue.removeNodeEntryFromMaxAge(ne);
            this.evict(ne);
         }
         else
         {
            break;
         }
      }

      it = lruQueue.iterateMaxAgeQueue();
      while (it.hasNext())
      {
         ne = (NodeEntry) it.next();
         if (isNodeInUseAndNotTimedOut(ne))
         {
            continue;
         }

         if (this.shouldEvictNode(ne))
         {
            it.remove();
            lruQueue.removeNodeEntryFromLRU(ne);
            this.evict(ne);
         }
         else
         {
            break;
         }
      }

      int maxNodes = this.getConfiguration().getMaxNodes();
      if (maxNodes <= 0)
      {
         return;
      }

      it = lruQueue.iterateLRUQueue();
      while (evictionQueue.getNumberOfNodes() > maxNodes)
      {
         ne = (NodeEntry) it.next();
         if (log.isTraceEnabled())
         {
            log.trace("Node " + ne.getFqn() + " will be evicted because of exceeding the maxNode limit." +
                  " maxNode: " + maxNodes + " but current queue size is: " + evictionQueue.getNumberOfNodes());
         }

         if (!this.isNodeInUseAndNotTimedOut(ne))
         {
            it.remove();
            lruQueue.removeNodeEntryFromMaxAge(ne);
            this.evict(ne);
         }
      }
   }

   protected LRUConfiguration getConfiguration()
   {
      return (LRUConfiguration) region.getEvictionConfiguration();
   }

}
