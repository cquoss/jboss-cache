package org.jboss.cache.eviction;

/**
 * Interface for all eviction algorithms.
 * <p/>
 * Note: None of the Eviction classes are thread safe. It is assumed that an individual instance of an EvictionPolicy/
 * EvictionAlgorithm/EvictionQueue/EvictionConfiguration are only operated on by one thread at any given time.
 *
 * @author Ben Wang 2-2004
 * @author Daniel Huang - dhuang@jboss.org - 10/2005
 * @version $Revision: 900 $
 */
public interface EvictionAlgorithm
{
   /**
    * Entry point for evictin algorithm. This is an api called by the EvictionTimerTask
    * to process the node events in waiting and actual pruning, if necessary.
    *
    * @param region Region that this algorithm will operate on.
    */
   void process(Region region) throws EvictionException;

   /**
    * Reset the whole eviction queue. Queue may needs to be reset due to corrupted state, for example.
    *
    * @param region Region that this algorithm will operate on.
    */
   void resetEvictionQueue(Region region);

   /**
    * Get the EvictionQueue implementation used by this algorithm.
    *
    * @return the EvictionQueue implementation.
    */
   EvictionQueue getEvictionQueue();

}
