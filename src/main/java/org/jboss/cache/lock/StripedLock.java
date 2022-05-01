/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2007, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.cache.lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;

import java.util.Iterator;
import java.util.List;

import EDU.oswego.cs.dl.util.concurrent.ReentrantWriterPreferenceReadWriteLock;

/**
 * A simple implementation of lock striping, using Fqns as the keys to lock on, primarily used to help make
 * {@link org.jboss.cache.loader.CacheLoader} implemtations thread safe.
 * <p/>
 * Backed by a set of {@link java.util.concurrent.locks.ReentrantReadWriteLock} instances, and using the {@link org.jboss.cache.Fqn}
 * hashcodes to determine buckets.
 * <p/>
 * Since buckets are used, it doesn't matter that the Fqn in question is not removed from the lock map when no longer in
 * use, since the Fqn is not referenced in this class.  Rather, the hash code is used.
 * <p/>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 */
public class StripedLock
{
   private static final int DEFAULT_CONCURRENCY = 20;
   private int lockSegmentMask;
   private int lockSegmentShift;

   ReentrantReadWriteLock[] sharedLocks;
   private Log log = LogFactory.getLog(StripedLock.class);

   /**
    * This constructor just calls {@link #StripedLock(int)} with a default concurrency value of 20.
    */
   public StripedLock()
   {
      this(DEFAULT_CONCURRENCY);
   }

   /**
    * Creates a new StripedLock which uses a certain number of shared locks across all elements that need to be locked.
    *
    * @param concurrency number of threads expected to use this class concurrently.
    */
   public StripedLock(int concurrency)
   {
      int tempLockSegShift = 0;
      int numLocks = 1;
      while (numLocks < concurrency)
      {
         ++tempLockSegShift;
         numLocks <<= 1;
      }
      lockSegmentShift = 32 - tempLockSegShift;
      lockSegmentMask = numLocks - 1;

      sharedLocks = new ReentrantReadWriteLock[numLocks];

      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new ReentrantReadWriteLock();
   }

   /**
    * Blocks until a lock is acquired.
    *
    * @param fqn       the Fqn to lock on
    * @param exclusive if true, a write (exclusive) lock is attempted, otherwise a read (shared) lock is used.
    */
   public void acquireLock(Fqn fqn, boolean exclusive)
   {
      ReentrantReadWriteLock lock = getLock(fqn);

      try
      {
         if (exclusive)
         {
            // allow for reentrancy
            lock.writeLock().acquire();
         }
         else
         {
            lock.readLock().acquire();
         }
      }
      catch (InterruptedException ex)
      {
         Thread.currentThread().interrupt(); // propagate without throwing, restoring the interrupted status
      }
   }

   /**
    * Releases a lock the caller may be holding.
    *
    * @param fqn the Fqn to release
    * @param exclusive if true, a write (exclusive) lock is released, otherwise a read (shared) lock is released.
    */
   public void releaseLock(Fqn fqn)
   {
      ReentrantReadWriteLock lock = getLock(fqn);
      if (lock.isWriteLockedByCurrentThread())
      {
         lock.writeLock().release();
         // check that we still don't have a stale WL
         if (lock.isWriteLockedByCurrentThread() && log.isWarnEnabled())
            log.warn("Write lock still exists on Fqn " + fqn + " for current thread.  Perhaps this was write-locked more than once?");
      }
      else
      {
         try
         {
            lock.readLock().release();
         }
         catch (IllegalMonitorStateException imse)
         {
            // perhaps the RL was already released earlier?
         }
      }
   }

   ReentrantReadWriteLock getLock(Object o)
   {
      return sharedLocks[hashToIndex(o)];
   }

   int hashToIndex(Object o)
   {
      return (hash(o) >>> lockSegmentShift) & lockSegmentMask;
   }

   /**
    * Returns a hash code for non-null Object x.
    * Uses the same hash code spreader as most other java.util hash tables, except that this uses the string representation
    * of the object passed in.
    *
    * @param x the object serving as a key
    * @return the hash code
    */
   int hash(Object x)
   {
      int h = x.toString().hashCode();
      h += ~(h << 9);
      h ^= (h >>> 14);
      h += (h << 4);
      h ^= (h >>> 10);
      return h;
   }

   /**
    * Releases locks on all fqns passed in.  Makes multiple calls to {@link #releaseLock(org.jboss.cache.Fqn)}.  This method is idempotent.
    *
    * @param fqns list of fqns
    * @see #releaseLock(org.jboss.cache.Fqn)
    */
   public void releaseAllLocks(List fqns)
   {
      Iterator fqnsIterator = fqns.iterator();

      while (fqnsIterator.hasNext())
      {
         releaseLock((Fqn) fqnsIterator.next());
      }
   }

   /**
    * Acquires locks on all fqns passed in.  Makes multiple calls to {@link #acquireLock(org.jboss.cache.Fqn,boolean)}
    *
    * @param fqns      list of fqns
    * @param exclusive whether locks are exclusive.
    * @see #acquireLock(org.jboss.cache.Fqn,boolean)
    */
   public void acquireAllLocks(List fqns, boolean exclusive)
   {
      Iterator fqnsIterator = fqns.iterator();

      while (fqnsIterator.hasNext())
      {
         acquireLock((Fqn) fqnsIterator.next(), exclusive);
      }
   }

   /**
    * A lock class that allows querying whether a WL is held by the current thread.
    */
   class ReentrantReadWriteLock extends ReentrantWriterPreferenceReadWriteLock
   {
      public boolean isWriteLockedByCurrentThread() {
         return activeWriter_ == Thread.currentThread();
      }
   }
}
