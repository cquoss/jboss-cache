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
 * FIFO Eviction Queue implementation for FIFO Policy.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 2054 $
 */
public class FIFOQueue implements EvictionQueue
{
   private Map nodeMap;
   private int numElements = 0;

   FIFOQueue()
   {
      nodeMap = new LinkedHashMap();
      // We use a LinkedHashMap here because we want to maintain FIFO ordering and still get the benefits of
      // O(n) = 1 for add/remove/search.
   }

   public NodeEntry getFirstNodeEntry()
   {
/*      Iterator it = nodeMap.keySet().iterator();
      if(it.hasNext()) {
         return (NodeEntry) nodeMap.get(it.next());
      }

      return null; */

      // this code path is *slightly* faster when profiling. 20ms faster iterating over 200000 entries in queue.
      if (nodeMap.size() > 0)
      {
         return (NodeEntry) nodeMap.values().iterator().next();
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
      NodeEntry e = (NodeEntry) nodeMap.remove(entry.getFqn());
      this.numElements -= e.getNumberOfElements();
   }

   public void addNodeEntry(NodeEntry entry)
   {
      if (!this.containsNodeEntry(entry))
      {
         entry.queue = this;
         nodeMap.put(entry.getFqn(), entry);
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

   public void modifyElementCount(int difference)
   {
      this.numElements += difference;
   }

   public void clear()
   {
      nodeMap.clear();
      this.numElements = 0;
   }

   public Iterator iterate()
   {
      return nodeMap.values().iterator();
   }
}
