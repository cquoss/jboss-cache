/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.jboss.cache.Fqn;

import java.util.Iterator;


/**
 * Eviction Queue interface defines a contract for the Eviction Queue implementations used by EvictionPolicies.
 * <p/>
 * Note: None of the Eviction classes are thread safe. It is assumed that an individual instance of an EvictionPolicy/
 * EvictionAlgorithm/EvictionQueue/EvictionConfiguration are only operated on by one thread at any given time.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 2054 $
 */
public interface EvictionQueue
{
   /**
    * Get the first entry in the queue.
    * <p/>
    * If there are no entries in queue, this method will return null.
    * <p/>
    * The first node returned is expected to be the first node to evict.
    *
    * @return first NodeEntry in queue.
    */
   public NodeEntry getFirstNodeEntry();

   /**
    * Retrieve a node entry by Fqn.
    * <p/>
    * This will return null if the entry is not found.
    *
    * @param fqn Fqn of the node entry to retrieve.
    * @return Node Entry object associated with given Fqn param.
    */
   public NodeEntry getNodeEntry(Fqn fqn);

   public NodeEntry getNodeEntry(String fqn);

   /**
    * Check if queue contains the given NodeEntry.
    *
    * @param entry NodeEntry to check for existence in queue.
    * @return true/false if NodeEntry exists in queue.
    */
   public boolean containsNodeEntry(NodeEntry entry);

   /**
    * Remove a NodeEntry from queue.
    * <p/>
    * If the NodeEntry does not exist in the queue, this method will return normally.
    *
    * @param entry The NodeEntry to remove from queue.
    */
   public void removeNodeEntry(NodeEntry entry);

   /**
    * Add a NodeEntry to the queue.
    *
    * @param entry The NodeEntry to add to queue.
    */
   public void addNodeEntry(NodeEntry entry);

   /**
    * Get the number of nodes in the queue.
    *
    * @return The number of nodes in the queue.
    */
   public int getNumberOfNodes();

   /**
    * Get the number of elements in the queue.
    *
    * @return The number of elements in the queue.
    */
   public int getNumberOfElements();

   public void modifyElementCount(int difference);

   public Iterator iterate();

   /**
    * Clear the queue.
    */
   public void clear();

}
