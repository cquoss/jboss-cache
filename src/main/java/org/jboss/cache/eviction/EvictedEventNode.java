/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.jboss.cache.Fqn;

/**
 * Value object used in evicted event node queue.
 *
 * @author Ben Wang 2-2004
 * @author Daniel Huang (dhuang@jboss.org)
 * @see Region
 */
public class EvictedEventNode
{
   // Signal the operation types.
   public static final int ADD_NODE_EVENT = 0;
   public static final int REMOVE_NODE_EVENT = 1;
   public static final int VISIT_NODE_EVENT = 2;
   public static final int ADD_ELEMENT_EVENT = 3;
   public static final int REMOVE_ELEMENT_EVENT = 4;
   public static final int MARK_IN_USE_EVENT = 5;
   public static final int UNMARK_USE_EVENT = 6;

   private Fqn fqn_;
   private int event_;
   private int elementDifference_;
   private boolean resetElementCount_;

   private long inUseTimeout;

   public EvictedEventNode(Fqn fqn, int event, int elementDifference)
   {
      setFqn(fqn);
      setEvent(event);
      setElementDifference(elementDifference);
   }

   public EvictedEventNode(Fqn fqn, int event)
   {
      setFqn(fqn);
      setEvent(event);
   }

   public long getInUseTimeout()
   {
      return inUseTimeout;
   }

   public void setInUseTimeout(long inUseTimeout)
   {
      this.inUseTimeout = inUseTimeout;
   }

   public boolean isResetElementCount()
   {
      return this.resetElementCount_;
   }

   public void setResetElementCount(boolean resetElementCount)
   {
      this.resetElementCount_ = resetElementCount;
   }

   public int getElementDifference()
   {
      return elementDifference_;
   }

   public void setElementDifference(int elementDifference_)
   {
      this.elementDifference_ = elementDifference_;
   }

   public Fqn getFqn()
   {
      return fqn_;
   }

   public void setFqn(Fqn fqn)
   {
      this.fqn_ = fqn;
   }

   public void setEvent(int event)
   {
      event_ = event;
   }

   public int getEvent()
   {
      return event_;
   }

   public String toString()
   {
      return "EvictedEN[fqn=" + fqn_ + " event=" + event_ + " diff=" + elementDifference_ + "]";
   }
}
