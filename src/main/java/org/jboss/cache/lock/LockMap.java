/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.CopyOnWriteArraySet;

// See IdentityLockPerfTest --
// oswego read/write acquire/release locks/sec: 2000000, 2840909
// Java   read/write acquire/release locks/sec: 2173913, 3048780
// about 7% improvement on single proc
// import java.util.concurrent.CopyOnWriteArraySet;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

/**
 * Provide lock ownership mapping.
 *
 * @author Ben Wang
 * @version $Id: LockMap.java 1519 2006-04-08 05:24:39Z genman $
 */
public class LockMap
{
   public static final int OWNER_ANY = 0;
   public static final int OWNER_READ = 1;
   public static final int OWNER_WRITE = 2;

   private Object writeOwner_=null;

   /**
    * Set of owners.  As this set is small (not like 100+) this structure is
    * sufficient.
    */
   private final Set readOwnerList_ = new CopyOnWriteArraySet();

   public LockMap()
   {
   }


   /**
    * Check whether this owner has reader or writer ownership.
    *
    * @param caller    the potential owner.  Cannot be <code>null</code>.
    * @param ownership Either <code>OWNER_ANY</code>, <code>OWNER_READ</code>,
    *                  or <code>OWNER_WRITE</code>.
    * @return
    * 
    * @throws NullPointerException if <code>caller</code> is <code>null</code>.
    */
   public boolean isOwner(Object caller, int ownership)
   {
      /* This method doesn't need to be synchronized; the thread is doing a simple read access (writer, readers)
         and only the current thread can *change* the writer or readers, so this cannot happen while we read.
      */

      switch (ownership) {
         case OWNER_ANY:
            return (writeOwner_ != null && caller.equals(writeOwner_) || readOwnerList_.contains(caller));
         case OWNER_READ:
            return (readOwnerList_.contains(caller));
         case OWNER_WRITE:
            return (writeOwner_ != null && caller.equals(writeOwner_));
         default:
      return false;
   }
   }



   /**
    * Adding a reader owner.
    *
    * @param owner
    */
   public void addReader(Object owner)
   {
      readOwnerList_.add(owner);
   }

   /**
    * Adding a writer owner.
    *
    * @param owner
    */
   public void setWriterIfNotNull(Object owner)
   {
      synchronized(this) {
         if(writeOwner_ != null)
            throw new IllegalStateException("there is already a writer holding the lock: " + writeOwner_);
         writeOwner_=owner;
      }
   }

   private Object setWriter(Object owner) {
      Object old;
      synchronized(this) {
         old=writeOwner_;
         writeOwner_=owner;
      }
      return old;
   }


   /**
    * Upgrading current reader ownership to writer one.
    *
    * @param owner
    * @return True if successful.
    */
   public boolean upgrade(Object owner) throws OwnerNotExistedException
   {
      boolean old_value = readOwnerList_.remove(owner);
      if(!old_value) // didn't exist in the list
         throw new OwnerNotExistedException("Can't upgrade lock. Read lock owner did not exist");
      setWriter(owner);
      return true;
   }

   /**
    * Returns an unmodifiable set of reader owner objects.
    */
   public Set readerOwners()
   {
      return Collections.unmodifiableSet(readOwnerList_);
   }

   public void releaseReaderOwners(LockStrategy lock)
   {
      int size = readOwnerList_.size();
      for (int i = 0; i < size; i++)
         lock.readLock().release();
   }

   /**
    * @return Writer owner object. Null if none.
    */
   public Object writerOwner()
   {
      return writeOwner_;
   }

   /**
    * Remove reader ownership.
    */
   public void removeReader(Object owner)
   {
      readOwnerList_.remove(owner);
   }

   /**
    * Remove writer ownership.
    */
   public void removeWriter()
   {
      synchronized(this) {
         writeOwner_=null;
      }
   }

   /**
    * Remove all ownership.
    */
   public void removeAll()
   {
      removeWriter();
      readOwnerList_.clear();
   }

   /**
    * Debugging information.
    *
    * @return
    */
   public String printInfo()
   {
      StringBuffer buf = new StringBuffer(64);
      buf.append("Read lock owners: ").append(readOwnerList_).append('\n');
      buf.append("Write lock owner: ").append(writeOwner_).append('\n');
      return buf.toString();
   }

   public boolean isReadLocked() {
      return !readOwnerList_.isEmpty();
   }
}
