/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.FIFOSemaphore;
import EDU.oswego.cs.dl.util.concurrent.Sync;

//import org.jboss.logging.Logger;

/**
 * Simple lock that does not differentiate read and write lock. All locks are obtained FIFO.
 * Just implements from Doug Lea's concurrent package. This class is used as a delegate for LockStrategy
 * is transaction isolation level.
 *
 * @author <a href="mailto:bwang00@sourceforge.net">Ben Wang</a> July 15, 2003
 * @version $Revision: 13 $
 */
public class SimpleLock
{
//    Log log=LogFactory.getLog(getClass());
   private int permits_;
   private FIFOSemaphore sem_;

   public SimpleLock()
   {
      permits_ = 1;
      sem_ = new FIFOSemaphore(permits_);
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#readLock()
    */
   public Sync readLock()
   {
      return sem_;
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#upgradeLockAttempt(long)
    */
   public Sync upgradeLockAttempt(long msecs)
   {
      return sem_;
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#writeLock()
    */
   public Sync writeLock()
   {
      return sem_;
   }
}
