/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.FIFOSemaphore;
import EDU.oswego.cs.dl.util.concurrent.NullSync;
import EDU.oswego.cs.dl.util.concurrent.Sync;

/**
 * Transaction isolation level of READ-UNCOMMITTED. Reads always succeed (NullLock), whereas writes are exclusive.
 * It prevents none of the dirty read, non-repeatable read, or phantom read.
 *
 * @author Ben Wang
 * @version $Revision: 2073 $
 */
public class LockStrategyReadUncommitted implements LockStrategy
{
   private FIFOSemaphore wLock_;
   private NullSync rLock_; // Null lock will always return true

   public LockStrategyReadUncommitted()
   {
      wLock_ = new FIFOSemaphore(1);
      rLock_ = new NullSync();
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#readLock()
    */
   public Sync readLock()
   {
      return rLock_;
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#upgradeLockAttempt(long)
    */
   public Sync upgradeLockAttempt(long msecs) throws UpgradeException
   {
      // Since write is exclusive, we need to obtain the write lock first
      // before we can return the upgrade
      try {
         wLock_.attempt(msecs);
      } catch (InterruptedException e) {
         throw new UpgradeException("Upgrade failed in " + msecs + " msecs", e);
      }
      return wLock_;
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#writeLock()
    */
   public Sync writeLock()
   {
      return wLock_;
   }
}
