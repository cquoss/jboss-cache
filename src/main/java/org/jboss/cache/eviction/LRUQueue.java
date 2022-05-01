/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.jboss.cache.Fqn;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU Eviction Queue implementation.
 * <p/>
 * This eviction queue will iterate properly through two sorted lists.
 * One sorted by maxAge and the other sorted by idleTime.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 2054 $
 */
public class LRUQueue implements EvictionQueue
{
   private Map maxAgeQueue;
   private Map lruQueue;
   private long alternatingCount = 0;
   private int numElements = 0;

   LRUQueue()
   {
      maxAgeQueue = new LinkedHashMap();
      lruQueue = new LinkedHashMap(16, 0.75f, true);
   }

   void reorderByLRU(Fqn fqn)
   {
      // leave the max age queue alone - it is like a fifo.

      // the lru queue is access ordered. meaning the most recently read item is moved to the bottom of the queue.
      // simply calling get against it visits it and will cause LinkedHashMap to move it to the bottom of the queue.
      lruQueue.get(fqn);
   }

   public NodeEntry getFirstNodeEntry()
   {
      // because the underlying queue is in two differently sorted queues, we alternate between them when calling
      // a generic getFirstNodeEntry.
      // we must alternate to keep things balanced when evicting nodes based on the maxNodes attribute. We don't
      // want to just prune from one queue but rather we want to be able to prune from both.
      NodeEntry ne;
      if (alternatingCount % 2 == 0)
      {
         ne = this.getFirstLRUNodeEntry();
         if (ne == null)
         {
            ne = this.getFirstMaxAgeNodeEntry();
         }
      }
      else
      {
         ne = this.getFirstMaxAgeNodeEntry();
         if (ne == null)
         {
            ne = this.getFirstLRUNodeEntry();
         }
      }
      alternatingCount++;
      return ne;
   }

   public NodeEntry getFirstLRUNodeEntry()
   {
      if (lruQueue.size() > 0)
      {
         return (NodeEntry) lruQueue.values().iterator().next();
      }

      return null;
   }

   public NodeEntry getFirstMaxAgeNodeEntry()
   {
      if (maxAgeQueue.size() > 0)
      {
         return (NodeEntry) maxAgeQueue.values().iterator().next();
      }

      return null;
   }

   public NodeEntry getNodeEntry(Fqn fqn)
   {
      return (NodeEntry) lruQueue.get(fqn);
   }

   public NodeEntry getNodeEntry(String fqn)
   {
      return this.getNodeEntry(Fqn.fromString(fqn));
   }

   public boolean containsNodeEntry(NodeEntry entry)
   {
      return this.maxAgeQueue.containsKey(entry.getFqn());
   }

   void removeNodeEntryFromLRU(NodeEntry entry)
   {
      Fqn fqn = entry.getFqn();
      lruQueue.remove(fqn);
   }

   void removeNodeEntryFromMaxAge(NodeEntry entry)
   {
      Fqn fqn = entry.getFqn();
      maxAgeQueue.remove(fqn);
   }

   public void removeNodeEntry(NodeEntry entry)
   {
      if (!this.containsNodeEntry(entry))
      {
         return;
      }
      Fqn fqn = entry.getFqn();
      NodeEntry ne1 = (NodeEntry) lruQueue.remove(fqn);
      NodeEntry ne2 = (NodeEntry) maxAgeQueue.remove(fqn);

      if (ne1 == null || ne2 == null)
      {
         throw new RuntimeException("The queues are out of sync.");
      }

      this.numElements -= ne1.getNumberOfElements();

   }

   public void addNodeEntry(NodeEntry entry)
   {
      if (!this.containsNodeEntry(entry))
      {
         Fqn fqn = entry.getFqn();
         entry.queue = this;
         maxAgeQueue.put(fqn, entry);
         lruQueue.put(fqn, entry);
         this.numElements += entry.getNumberOfElements();
      }
   }

   public int getNumberOfNodes()
   {
      return maxAgeQueue.size();
   }

   public int getNumberOfElements()
   {
      return this.numElements;
   }

   public void clear()
   {
      maxAgeQueue.clear();
      lruQueue.clear();
      this.numElements = 0;
   }

   public void modifyElementCount(int difference)
   {
      this.numElements += difference;
   }

   public Iterator iterate()
   {
      return lruQueue.values().iterator();
   }

   final Iterator iterateMaxAgeQueue()
   {
      return maxAgeQueue.values().iterator();
   }

   final Iterator iterateLRUQueue()
   {
      return lruQueue.values().iterator();
   }

}
