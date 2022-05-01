/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.Sync;

/**
 * Interface to specify lock strategy, e.g., for different isolation levels.
 *
 * @author Ben Wang
 */
public interface LockStrategy
{
   /**
    * Return a read lock object.
    *
    * @return Sync (@see EDU.oswego.cs.dl.util.concurrent.Sync)
    */
   Sync readLock();


   /**
    * Return a write lock object.
    *
    * @return Sync (@see EDU.oswego.cs.dl.util.concurrent.Sync)
    */
   Sync writeLock();

   /**
    * Attempt to upgrade the current read lock to write lock with
    * <code>msecs</code> timeout.
    *
    * @param msecs Timeout in milliseconds.
    * @return Sync object. Will return null if timeout or failed.
    */
   Sync upgradeLockAttempt(long msecs) throws UpgradeException;
}
