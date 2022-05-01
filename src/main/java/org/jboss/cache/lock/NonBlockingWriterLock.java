/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.lock;

//import EDU.oswego.cs.dl.util.concurrent.Sync;
//import EDU.oswego.cs.dl.util.concurrent.Semaphore;
//import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;



/**
 * NonBlockingWriterLock is a read/write lock (with upgrade) that has
 * non-blocking write lock acquisition on existing read lock(s).
 * <p>Note that the write lock is exclusive among write locks, e.g.,
 * only one write lock can be granted at one time, but the write lock
 * is independent of the read locks. For example,
 * a read lock to be acquired will be blocked if there is existing write lock, but
 * will not be blocked if there are mutiple read locks already granted to other
 * owners. On the other hand, a write lock can be acquired as long as there
 * is no existing write lock, regardless how many read locks have been
 * granted.
 *
 * @author Ben Wang
 * @version $Id: NonBlockingWriterLock.java 13 2005-04-05 17:19:48Z belaban $
 */
public class NonBlockingWriterLock extends ReadWriteLockWithUpgrade
{

   // Only need to overwrite this method so WL is not blocked on RL.
   protected synchronized boolean startWrite()
   {
      boolean allowWrite = (activeWriter_ == null);
      if (allowWrite) activeWriter_ = Thread.currentThread();
      return allowWrite;
   }
}
