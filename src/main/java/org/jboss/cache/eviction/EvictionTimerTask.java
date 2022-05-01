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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimerTask;

/**
 * Timer threads to do periodic node clean up by running the eviction policy.
 *
 * @author Ben Wang 2-2004
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 1424 $
 */
public class EvictionTimerTask extends TimerTask
{
   private Log log = LogFactory.getLog(EvictionTimerTask.class);

   private final Set processedRegions;

   public EvictionTimerTask()
   {
      // synchronized set because we need to maintain thread safety
      // for dynamic configuration purposes.
      processedRegions = Collections.synchronizedSet(new HashSet());
   }

   /**
    * Add a Region to process by the Eviction Thread.
    *
    * @param region Region to process.
    */
   public void addRegionToProcess(Region region)
   {
      processedRegions.add(region);
   }

   /**
    * Remove a Region to process from the Eviction thread.
    *
    * @param region
    */
   public void removeRegionToProcess(Region region)
   {
      processedRegions.remove(region);
   }

   /**
    * Run the eviction thread.
    * <p/>
    * This thread will synchronize the set of regions and iterate through every Region registered w/ the
    * Eviction thread. It also synchronizes on each individual region as it is being processed.
    */
   public void run()
   {
      synchronized (processedRegions)
      {
         Iterator it = processedRegions.iterator();
         while (it.hasNext())
         {
            final Region region = (Region) it.next();
            final EvictionPolicy policy = region.getEvictionPolicy();

            synchronized (region)
            {
               final EvictionAlgorithm algo = policy.getEvictionAlgorithm();
               try
               {
                  algo.process(region);
               }
               catch (EvictionException e)
               {
                  log.error("run(): error processing eviction with exception: " + e.toString()
                        + " will reset the eviction queue list.");
                  region.resetEvictionQueues();
                  log.debug("trace", e);
               }
            }
         }
      }
   }
}
