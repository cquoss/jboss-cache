/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.NullSync;
import EDU.oswego.cs.dl.util.concurrent.Sync;

/**
 * Transaction isolation level of None.
 *
 * @author Lari Hotari
 * @version $Revision: 2073 $
 */
public class LockStrategyNone implements LockStrategy
{
   private NullSync nullLock_;

   public LockStrategyNone()
   {
      nullLock_ = new NullSync();
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#readLock()
    */
   public Sync readLock()
   {
      return nullLock_;
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#upgradeLockAttempt(long)
    */
   public Sync upgradeLockAttempt(long msecs) throws UpgradeException
   {
      return nullLock_;
   }

   /**
    * @see org.jboss.cache.lock.LockStrategy#writeLock()
    */
   public Sync writeLock()
   {
      return nullLock_;
   }
}

