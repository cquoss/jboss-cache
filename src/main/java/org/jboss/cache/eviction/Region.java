/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.eviction;

import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;


/**
 * A region is a collection of tree cache nodes that share the same eviction
 * policy configurations, e.g., maxNodes, etc. The region is specified via
 * Fqn.
 *
 * @author Ben Wang 2-2004
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 2054 $
 */
public class Region
{
   // Region Fqn
   private Fqn fqn_;
   // policy used by this region.
   private EvictionPolicy policy_;
   // Eviction configuration object used by this region.
   private EvictionConfiguration configuration_;

   // These queues can do put and take concurrently.
   protected BoundedLinkedQueue nodeEventQueue_;
   protected Log log_ = LogFactory.getLog(Region.class);

   // Count of how many times between attempt to check capacity
   protected int checkCapacityCount = 0;

   // Added capacity warning threshold constant with correct calculation. Plus 100 to be on the safe side.
   private final static int CAPACITY_WARN_THRESHOLD = (98 * RegionManager.CAPACITY) / 100 - 100;

   /**
    * Default to package namespace on purpose so no one outside the package can instantiate it,
    */
   Region()
   {
      if (CAPACITY_WARN_THRESHOLD <= 0)
      {
         throw new RuntimeException("Region.Region(): CAPACITY_WARN_THRESHOLD constant used in eviction is smaller than 1.");
      }
      createQueue();
   }

   void createQueue()
   {
      nodeEventQueue_ = new BoundedLinkedQueue(RegionManager.CAPACITY);
   }

   Region(String fqn, EvictionPolicy policy, EvictionConfiguration config)
   {
      this(Fqn.fromString(fqn), policy, config);
   }

   Region(Fqn fqn, EvictionPolicy policy, EvictionConfiguration config)
   {
      fqn_ = fqn;
      policy_ = policy;
      configuration_ = config;

      createQueue();
   }

   public EvictionConfiguration getEvictionConfiguration()
   {
      return this.configuration_;
   }

   public void setEvictionConfiguration(EvictionConfiguration configuration)
   {
      this.configuration_ = configuration;
   }

   public EvictionPolicy getEvictionPolicy()
   {
      return policy_;
   }

   /**
    * Returns the region as a string with a / at the end.
    */
   public String getFqn()
   {
      return fqn_.toString() + Fqn.SEPARATOR;
   }

   /**
    * Returns the region as a Fqn object.
    */
   public Fqn getFqnObject()
   {
      return fqn_;
   }

   public void setAddedNode(Fqn fqn)
   {
      putNodeEvent(fqn, EvictedEventNode.ADD_NODE_EVENT);
   }

   public void setRemovedNode(Fqn fqn)
   {
      putNodeEvent(fqn, EvictedEventNode.REMOVE_NODE_EVENT);
   }

   public void setVisitedNode(Fqn fqn)
   {
      putNodeEvent(fqn, EvictedEventNode.VISIT_NODE_EVENT);
   }

   public void putNodeEvent(Fqn fqn, int event)
   {
      this.putNodeEvent(new EvictedEventNode(fqn, event));
   }

   public void putNodeEvent(EvictedEventNode event)
   {
      try
      {
         // Don't check capacity every time as this is an expensive operation for
         // bounded buffer type objects in this situation
         if (++checkCapacityCount > 100)
         {
            checkCapacityCount = 0;
            if (nodeEventQueue_.size() > (CAPACITY_WARN_THRESHOLD))
            {
               log_.warn("putNodeEvent(): eviction node event queue size is at 98% threshold value of capacity: "
                     + RegionManager.CAPACITY + " You will need to reduce the wakeUpIntervalSeconds parameter.");
            }
         }
         nodeEventQueue_.put(event);
      }
      catch (InterruptedException e)
      {
         log_.debug("give up put", e);
      }
   }

   /**
    * Take the last node from node queue. It will also
    * remove it from the queue.
    *
    * @return The EvictedEventNode
    */
   public EvictedEventNode takeLastEventNode()
   {
      try
      {
         return (EvictedEventNode) nodeEventQueue_.poll(0);
      }
      catch (InterruptedException e)
      {
         log_.debug("trace", e);
      }
      return null;
   }

   public int nodeEventQueueSize()
   {
      return nodeEventQueue_.size();
   }

   public void resetEvictionQueues()
   {
      BoundedLinkedQueue q1 = nodeEventQueue_;
      log_.info("reseteEvictionQueues(): node queue size: " + q1.size() +
            " region name: " + fqn_);
      createQueue();
      // Help to recycle
      for (int i = 0; i < q1.size(); i++)
      {
         try
         {
            q1.take();
         }
         catch (InterruptedException e)
         {
            e.printStackTrace();
         }
      }
   }

   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final Region region = (Region) o;
      return fqn_.equals(region.fqn_);
   }

   public int hashCode()
   {
      return fqn_.hashCode();
   }

   /**
    * @deprecated DO NOT USE. THIS IS PROVIDED FOR EJB3 INTEGRATION WITH LEGACY JBCache 1.2 API
    */
   public void setTimeToLiveSeconds(long timeToLive)
   {
      if (this.getEvictionConfiguration() instanceof LRUConfiguration)
      {
         int ttl = new Long(timeToLive).intValue();
         ((LRUConfiguration) this.getEvictionConfiguration()).setTimeToLiveSeconds(ttl);
      }
      else
      {
         throw new RuntimeException("Incorrect usage of a deprecated API!!!!");
      }
   }

   /**
    * @deprecated DO NOT USE. THIS IS PROVIDED FOR EJB3 INTEGRATION WITH LEGACY JBCache 1.2 API
    */
   public void setMaxNodes(int maxSize)
   {
      if (this.getEvictionConfiguration() instanceof LRUConfiguration)
      {
         ((LRUConfiguration) this.getEvictionConfiguration()).setMaxNodes(maxSize);
      }
      else
      {
         throw new RuntimeException("Incorrect usage of a deprecated API!!!!");
      }

   }
}
