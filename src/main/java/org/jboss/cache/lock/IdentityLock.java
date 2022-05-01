/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.Sync;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Lock object which grants and releases locks, and associates locks with
 * <em>owners</em>.  The methods to acquire and release a lock require an owner
 * (Object). When a lock is acquired, we store the current owner with the lock.
 * When the same owner (<em>but possibly running in a different thread</em>)
 * wants to acquire the lock, it is immediately granted. When an owner
 * different from the one currently owning the lock wants to release the lock,
 * we do nothing (no-op).
 * <p>
 * Consider the following example:
 * <pre>
 * FIFOSemaphore lock=new FIFOSemaphore(1);
 * lock.acquire();
 * lock.acquire();
 * lock.release();
 * </pre>
 * This would block on the second <tt>acquire()</tt> (although we currently already hold
 * the lock) because the lock only has 1 permit. So <tt>IdentityLock</tt> will allow the
 * following code to work properly:
 * <pre>
 * IdentityLock lock=new IdentityLock();
 * lock.readLock().acquire(this, timeout);
 * lock.writeLock().acquire(this, timeout);
 * lock.release(this);
 * </pre>
 * If the owner is null, then the current thread is taken by default. This allow the following
 * code to work:
 * <pre>
 * IdentityLock lock=new IdentityLock();
 * lock.readLock().attempt(null, timeout);
 * lock.writeLock().attempt(null, timeout);
 * lock.release(null);
 * </pre>
 * <br/>
 * Note that the Object given as owner is required to implement {@link Object#equals}. For
 * a use case see the examples in the testsuite.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 11, 2003
 * @author Ben Wang July 2003
 * @version $Revision: 3990 $
 */
public class IdentityLock
{

   private static final Log log = LogFactory.getLog(IdentityLock.class);
   private static boolean trace = log.isTraceEnabled();
   private final LockStrategy lock_;
   private final LockMap map_;
   private final boolean mustReacquireRead_;
   private Fqn fqn_;

   /**
    * Constructs a new lock.
    * @param cache cache for deciding lock strategy
    * @param fqn ignored.
    */
   public IdentityLock(TreeCache cache, Fqn fqn)
   {
      this(cache);
      this.fqn_ = fqn;
   }

   /**
    * Constructs a new lock.
    * @param cache cache for deciding lock strategy
    * 
    * TODO remove this constructor
    */
   public IdentityLock(TreeCache cache)
   {
      if(cache == null) {
         if(trace) {
            log.trace("Cache instance is null. Use default lock strategy");
         }
         lock_ = LockStrategyFactory.getLockStrategy();
         // TODO This is ugly; builds in the assumption that default strategy
         // is REPEATABLE_READ
         mustReacquireRead_ = false;
      } else {         
         IsolationLevel level = cache.getIsolationLevelClass();
         // Repeatable Read *is* the default ...
         lock_ = LockStrategyFactory.getLockStrategy(level == null ? IsolationLevel.REPEATABLE_READ : level);
         mustReacquireRead_ = (level == IsolationLevel.READ_COMMITTED);
      }
      map_ = new LockMap();
   }


   /** For testing only */
   public IdentityLock(IsolationLevel level) {
      lock_ = LockStrategyFactory.getLockStrategy(level);
      mustReacquireRead_ = (level == IsolationLevel.READ_COMMITTED);
      map_ = new LockMap();
      fqn_ = Fqn.ROOT;
   }

   /**
    * Returns the FQN for this lock, may be <code>null</code>.
    */
   public Fqn getFqn() {
      return fqn_;
   }

   /**
    * Return a copy of the reader lock owner in List. Size is zero is not available. Note that this list
    * is synchronized.
    *
    * @return Set of readers
    */
   public Set getReaderOwners()
   {
      return map_.readerOwners();
   }

   /**
    * Return the writer lock owner object. Null if not available.
    *
    * @return Object owner
    */
   public Object getWriterOwner()
   {
      return map_.writerOwner();
   }

   /**
    * Acquire a write lock with a timeout of <code>timeout</code> milliseconds.
    * Note that if the current owner owns a read lock, it will be upgraded
    * automatically. However, if upgrade fails, i.e., timeout, the read lock will
    * be released automatically.
    *
    * @param caller   Can't be null.
    * @param timeout
    * @throws LockingException
    * @throws TimeoutException
    * @return boolean True if lock was acquired and was not held before, false if lock was held
    */
   public boolean acquireWriteLock(Object caller, long timeout) throws LockingException, TimeoutException, InterruptedException
   {
      if (caller == null) {
         throw new IllegalArgumentException("acquireWriteLock(): null caller");
      }

      if (map_.isOwner(caller, LockMap.OWNER_WRITE)) {
         if (trace)
            log.trace("acquireWriteLock(): caller already owns lock for " + getFqn() + " (caller=" + caller + ')');
         return false; // owner already has the lock
      }

      // Check first if we need to upgrade
      if (map_.isOwner(caller, LockMap.OWNER_READ)) {
         // Currently is a reader owner. Obtain the writer ownership then.
         Sync wLock;
         try {
            if(trace)
               log.trace("upgrading RL to WL for " + caller + ", timeout=" + timeout + ", locks: " + map_.printInfo());
            wLock = lock_.upgradeLockAttempt(timeout);
         } catch (UpgradeException ue) {
            String errStr="acquireWriteLock(): lock upgrade failed for " + getFqn() + " (caller=" + caller + ", lock info: " + toString(true) + ')';
            log.trace(errStr, ue);
            throw new UpgradeException(errStr, ue);
         }
         if (wLock == null) {
            release(caller);   // bug fix: remember to release the read lock before throwing the exception
            map_.removeReader(caller);
            String errStr="upgrade lock for " + getFqn() + " could not be acquired after " + timeout + " ms." +
                  " Lock map ownership " + map_.printInfo() + " (caller=" + caller + ", lock info: " + toString(true) + ')';
            log.trace(errStr);
            throw new UpgradeException(errStr);
         }
         try {
            if (trace)
               log.trace("upgrading lock for " + getFqn());
            map_.upgrade(caller);
         } catch (OwnerNotExistedException ex) {
            throw new UpgradeException("Can't upgrade lock to WL for " + getFqn() + ", error in LockMap.upgrade(): " + ex);
         }
      }
      else {
         // Not a current reader owner. Obtain the writer ownership then.
         boolean rc = lock_.writeLock().attempt(timeout);

         // we don't need to synchronize from here on because we own the semaphore
         if (rc == false) {
            String errStr = "write lock for " + getFqn() + " could not be acquired after " + timeout + " ms. " +
                  "Locks: " + map_.printInfo() + " (caller=" + caller + ", lock info: " + toString(true) + ')';
            log.trace(errStr);
            throw new TimeoutException(errStr);
         }
         map_.setWriterIfNotNull(caller);
      }
      return true;
   }

   /**
    * Acquire a read lock with a timeout period of <code>timeout</code> milliseconds.
    *
    * @param caller   Can't be null.
    * @param timeout
    * @throws LockingException
    * @throws TimeoutException
    * @return boolean True if lock was acquired and was not held before, false if lock was held
    */
   public boolean acquireReadLock(Object caller, long timeout) 
      throws LockingException, TimeoutException, InterruptedException
   {
      boolean rc;

      if (caller == null) {
         throw new IllegalArgumentException("owner is null");
      }

      boolean hasRead     = false;
      boolean hasRequired = false;
      if (mustReacquireRead_)
      {
         hasRequired = map_.isOwner(caller, LockMap.OWNER_WRITE);
         if (!hasRequired)
            hasRead = map_.isOwner(caller, LockMap.OWNER_READ);
      }
      else if (map_.isOwner(caller, LockMap.OWNER_ANY)) {
         hasRequired = true;
      }
         
      if (hasRequired) {
         if (trace) {
            StringBuffer sb=new StringBuffer(64);
            sb.append("acquireReadLock(): caller ").append(caller).append(" already owns lock for ").append(getFqn());
            log.trace(sb.toString());
         }
         return false; // owner already has the lock
      }

      rc = lock_.readLock().attempt(timeout);

      // we don't need to synchronize from here on because we own the semaphore
      if (rc == false) {
         StringBuffer sb = new StringBuffer();
         sb.append("read lock for ").append(getFqn()).append(" could not be acquired by ").append(caller);
         sb.append(" after ").append(timeout).append(" ms. " + "Locks: ").append(map_.printInfo());
         sb.append(", lock info: ").append(toString(true));
         String errMsg = sb.toString();
         log.trace(errMsg);
         throw new TimeoutException(errMsg);
      }
      
      // Only add to the map if we didn't already have the lock
      if (!hasRead)
         map_.addReader(caller); // this is synchronized internally, we don't need to synchronize here
      return true;
   }

   /**
    * Release the lock held by the owner.
    *
    * @param caller Can't be null.
    */
   public void release(Object caller)
   {
      if (caller == null) {
         throw new IllegalArgumentException("IdentityLock.release(): null owner object.");
      }

      // Check whether to release reader or writer lock.
      if (map_.isOwner(caller, LockMap.OWNER_READ)) {
         map_.removeReader(caller);
         lock_.readLock().release();
      }
      else if (map_.isOwner(caller, LockMap.OWNER_WRITE)) {
         map_.removeWriter();
         lock_.writeLock().release();
      }
   }

   /**
    * Release all locks associated with this instance.
    */
   public void releaseAll()
   {
      Collection col;
      try {
         if ((map_.writerOwner()) != null) {
            // lock_.readLock().release();
            lock_.writeLock().release();
         }

         map_.releaseReaderOwners(lock_);
      }
      finally {
         map_.removeAll();
      }
   }

   /**
    * Same as releaseAll now.
    */
   public void releaseForce()
   {
      releaseAll();
   }

   /**
    * Check if there is a read lock.
    */
   public boolean isReadLocked()
   {
      return map_.isReadLocked();
   }

   /**
    * Check if there is a write lock.
    */
   public boolean isWriteLocked()
   {
      return map_.writerOwner() != null;
   }

   /**
    * Check if there is a read or write lock
    */
   public boolean isLocked()
   {
      return isReadLocked() || isWriteLocked();
   }

   /**
    * Am I a lock owner?
    *
    * @param o
    */
   public boolean isOwner(Object o)
   {
      return map_.isOwner(o, LockMap.OWNER_ANY);
   }

   public String toString()
   {
      return toString(false);
   }

   public String toString(boolean print_lock_details)
   {
      StringBuffer sb=new StringBuffer();
      toString(sb, print_lock_details);
      return sb.toString();
   }

   public void toString(StringBuffer sb)
   {
      toString(sb, false);
   }

   public void toString(StringBuffer sb, boolean print_lock_details)
   {
      boolean printed_read_owners=false;
      Collection read_owners=lock_ != null ? getReaderOwners() : null;
      if(read_owners != null && read_owners.size() > 0) {
         // Fix for JBCACHE-310 -- can't just call new ArrayList(read_owners) :(
         // Creating the ArrayList and calling addAll doesn't work either
         // Looking at the details of how this is implemented vs. the 2 prev
         // options, this doesn't look like it should be slower
         Iterator iter = read_owners.iterator();
         read_owners = new ArrayList(read_owners.size());
         while(iter.hasNext())
            read_owners.add(iter.next());
         
         sb.append("read owners=").append(read_owners);
         printed_read_owners=true;
      }
      else
         read_owners=null;

      Object write_owner=lock_ != null ? getWriterOwner() : null;
      if(write_owner != null) {
         if(printed_read_owners)
            sb.append(", ");
         sb.append("write owner=").append(write_owner);
      }
      if(read_owners == null && write_owner == null)
      {
        sb.append("<unlocked>");
      }
      if(print_lock_details)
      {
         sb.append(" (").append(lock_.toString()).append(')');
      }
   }
}
