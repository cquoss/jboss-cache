/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import java.util.Set;

import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;

/**
 * Generic eviction policy interface.
 * <p/>
 * None of the Eviction classes are thread safe. It is assumed that an individual instance of an EvictionPolicy/
 * EvictionAlgorithm/EvictionQueue/EvictionConfiguration are only operated on by one thread at any given time.
 *
 * @author Ben Wang 2-2004
 * @author Daniel Huang - dhuang@jboss.org - 10/2005
 */
public interface EvictionPolicy
{
   /**
    * Evict a node form the underlying cache.
    *
    * @param fqn DataNode corresponds to this fqn.
    * @throws Exception
    */
   void evict(Fqn fqn) throws Exception;

   /**
    * Return children names as Objects
    *
    * @param fqn
    * @return Child names under given fqn
    */
   Set getChildrenNames(Fqn fqn);

   /**
    * Is this a leaf node?
    *
    * @param fqn
    * @return true/false if leaf node.
    */
   boolean hasChild(Fqn fqn);

   Object getCacheData(Fqn fqn, Object key);

   /**
    * Method called to configure this implementation.
    */
   void configure(TreeCache cache);

   /**
    * Get the associated EvictionAlgorithm used by the EvictionPolicy.
    * <p/>
    * This relationship should be 1-1.
    *
    * @return An EvictionAlgorithm implementation.
    */
   EvictionAlgorithm getEvictionAlgorithm();

   /**
    * The EvictionConfiguration implementation class used by this EvictionPolicy.
    *
    * @return EvictionConfiguration implementation class.
    */
   Class getEvictionConfigurationClass();

   /**
    * This method will be invoked prior to an event being processed for a node with the
    * specified Fqn. </p>
    * 
    * This method provides a way to optimize the performance of eviction by 
    * signalling that the node associated with the specified Fqn should not be
    * subject to normal eviction processing.</p>

    * If this method returns false then then event is processed normally
    * and eviction processing for the node continues. As a result,
    * EvictionPolicy.evict() may be invoked at some later point based on the
    * particular algorirthm of the eviction policy.
    * </p>
    * 
    * If this method returns true, then the event is ignored and eviction
    * processing is bypassed for this node. As a result, EvictionPolicy.evict()
    * will never be invoked for this node.
    * </p>
    * 
    * @param fqn The Fqn of the node associated with the event.
    * @return true to ignore events for this Fqn. false to process events normally.
    */
   boolean canIgnoreEvent(Fqn fqn);
}
