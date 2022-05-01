/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.Sync;

/**
 * Transaction isolation level of Repeatable_Read. It prevents dirty read and non-repeatable read.
 * <p> Dirty read allows (t1) write and then (t2) read within two separate threads, all without
 * transaction commit. </p>
 * <p> Non-repeatable allows read allows (t1) read, (t2) write, and then (t1) read, all without
 * transaction commit. </p>
 *
 * @author Ben Wang
 * @version $Revision: 2073 $
 */
public class LockStrategyRepeatableRead implements LockStrategy
{
   private ReadWriteLockWithUpgrade lock_; // Delegate is ReadWriteLockWithUpgrade

   public LockStrategyRepeatableRead()
   {
      lock_ = new ReadWriteLockWithUpgrade();
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#readLock()
    */
   public Sync readLock()
   {
      return lock_.readLock();
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#upgradeLockAttempt(long)
    */
   public Sync upgradeLockAttempt(long msecs)  throws UpgradeException
   {
      return lock_.upgradeLockAttempt(msecs);
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#writeLock()
    */
   public Sync writeLock() 
   {
      return lock_.writeLock();
   }


   public String toString()
   {
      return lock_ != null? lock_.toString() : null;
   }
}
