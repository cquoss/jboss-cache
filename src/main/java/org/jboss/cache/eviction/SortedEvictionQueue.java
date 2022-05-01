/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

/**
 * Sorted Eviction Queue implementation.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 900 $
 */
public interface SortedEvictionQueue extends EvictionQueue
{
   /**
    * Provide contract to resort a sorted queue.
    */
   public void resortEvictionQueue();
}
