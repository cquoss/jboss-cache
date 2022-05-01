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
 * Lock strategy of Serializable that prevents dirty read, non-repeatable read, and
 * phantom read.
 * <p> Dirty read allows (t1) write and then (t2) read within two separate threads, all without
 * transaction commit. </p>
 * <p> Non-repeatable read allows (t1) read, (t2) write, and then (t1) read, all without
 * transaction commit. </p>
 * <p> Phantom read allows (t1) read n rows, (t2) insert k rows, and (t1) read n+k rows.</p>
 *
 * @author <a href="mailto:bwang00@sourceforge.net">Ben Wang</a> July 15, 2003
 * @version $Revision: 13 $
 */
public class LockStrategySerializable implements LockStrategy
{
//    Log log=LogFactory.getLog(getClass());
   private int permits_;
   private FIFOSemaphore sem_;

   public LockStrategySerializable()
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
   public Sync upgradeLockAttempt(long msecs) throws UpgradeException
   {
      // If we come to this far, that means the thread owns a rl already
      // so we just return the same lock
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
