/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.jboss.cache.Fqn;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * LFUQueue EvictionQueue implementation for LFU Policy.
 * <p/>
 * The queue is sorted in least frequently used order.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 2054 $
 */
public class LFUQueue implements SortedEvictionQueue
{
   private Map nodeMap;
   private LinkedList evictionList;
   private Comparator comparator;

   private Set removalQueue;
   private int numElements = 0;

   LFUQueue()
   {
      nodeMap = new HashMap();
      comparator = new LFUComparator();
      evictionList = new LinkedList();
      removalQueue = new HashSet();
   }

   /**
    * Return the first node to evict.
    * <p/>
    * This method will return the least frequently used entry in the queue.
    */
   public NodeEntry getFirstNodeEntry()
   {
      try
      {
         NodeEntry ne;
         while ((ne = (NodeEntry) evictionList.getFirst()) != null)
         {
            if (removalQueue.contains(ne))
            {
               evictionList.removeFirst();
               removalQueue.remove(ne);
            }
            else
            {
               break;
            }
         }
         return ne;
      }
      catch (NoSuchElementException e)
      {
         //
      }

      return null;
   }

   public NodeEntry getNodeEntry(Fqn fqn)
   {
      return (NodeEntry) nodeMap.get(fqn);
   }

   public NodeEntry getNodeEntry(String fqn)
   {
      return this.getNodeEntry(Fqn.fromString(fqn));
   }

   public boolean containsNodeEntry(NodeEntry entry)
   {
      Fqn fqn = entry.getFqn();
      return this.getNodeEntry(fqn) != null;
   }

   public void removeNodeEntry(NodeEntry entry)
   {
      NodeEntry ne = (NodeEntry) nodeMap.remove(entry.getFqn());
      if (ne != null)
      {
         // don't remove directly from the LinkedList otherwise we will incur a O(n) = n
         // performance penalty for every removal! In the prune method for LFU, we will iterate the LinkedList through ONCE
         // doing a single O(n) = n operation and removal. This is much preferred over running O(n) = n every single time
         // remove is called. There is also special logic in the getFirstNodeEntry that will know to check
         // the removalQueue before returning.
         this.removalQueue.add(ne);
/*         if(!evictionList.remove(ne)) {
            throw new RuntimeException("");
         } */
         this.numElements -= ne.getNumberOfElements();
      }
   }

   public void addNodeEntry(NodeEntry entry)
   {
      if (!this.containsNodeEntry(entry))
      {
         Fqn fqn = entry.getFqn();
         entry.queue = this;
         nodeMap.put(fqn, entry);
         evictionList.add(entry);
         this.numElements += entry.getNumberOfElements();
      }
   }

   public int getNumberOfNodes()
   {
      return nodeMap.size();
   }

   public int getNumberOfElements()
   {
      return this.numElements;
   }

   public void clear()
   {
      nodeMap.clear();
      evictionList.clear();
      removalQueue.clear();
      this.numElements = 0;
   }

   public void resortEvictionQueue()
   {
      Collections.sort(evictionList, comparator);
   }

   public void modifyElementCount(int difference)
   {
      this.numElements += difference;
   }

   void prune()
   {
      Iterator it = this.iterate();
      while (it.hasNext() && removalQueue.size() > 0)
      {
         if (removalQueue.remove(it.next()))
         {
            it.remove();
         }
      }
   }

   // provided as friend access for unit testing only.
   final List getEvictionList()
   {
      return this.evictionList;
   }

   // provided as friend access for unit testing only.
   final Set getRemovalQueue()
   {
      return this.removalQueue;
   }

   public Iterator iterate()
   {
      return evictionList.iterator();
   }

   /**
    * Comparator class for LFU.
    * <p/>
    * This class will sort the eviction queue in the correct eviction order.
    * The top of the list should evict before the bottom of the list.
    * <p/>
    * The sort is based on ascending order of nodeVisits.
    * <p/>
    * Note: this class has a natural ordering that is inconsistent with equals as defined by the java.lang.Comparator
    * contract.
    */
   static class LFUComparator implements Comparator
   {
      LFUComparator()
      {
      }

      public int compare(Object o, Object o1)
      {
         if (o.equals(o1))
         {
            return 0;
         }
         NodeEntry ne = (NodeEntry) o;
         NodeEntry ne2 = (NodeEntry) o1;

         int neNodeHits = ne.getNumberOfNodeVisits();
         int ne2NodeHits = ne2.getNumberOfNodeVisits();

         if (neNodeHits > ne2NodeHits)
         {
            return 1;
         }
         else if (neNodeHits < ne2NodeHits)
         {
            return -1;
         }
         else if (neNodeHits == ne2NodeHits)
         {
            return 0;
         }

         throw new RuntimeException("Should never reach this condition");
      }
   }

}

