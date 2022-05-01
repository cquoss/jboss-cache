/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.Sync;

/**
 * Transaction isolation level of READ_COMMITTED. Similar to read/write lock, but writers are not blocked on readers
 * It prevents dirty read only.
 * <p> Dirty read allows (t1) write and then (t2) read within two separate threads, all without
 * transaction commit. </p>
 *
 * @author Ben Wang
 * @version $Revision: 2073 $
 */
public class LockStrategyReadCommitted implements LockStrategy
{
   private NonBlockingWriterLock lock_;

   public LockStrategyReadCommitted()
   {
      lock_ = new NonBlockingWriterLock();
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
   public Sync upgradeLockAttempt(long msecs) throws UpgradeException
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
}
