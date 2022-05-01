/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.lock;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.Sync;

/*
 * <p> This class is similar to PreferredWriterReadWriteLock except that
 * the read lock is upgradable to write lock. I.e., when a user calls
 * upgradeLock(), it will release the read lock, wait for
 * all the read locks to clear and obtain a write lock afterwards. In
 * particular, the write lock is obtained with priority to prevent deadlock
 * situation. The current design is based in part from Doug Lea's
 * PreferredWriterReadWriteLock.
 *
 * <p>Note that the pre-requisite to use upgrade lock is the pre-existing of a read
 * lock. Otherwise, a RuntimeException will be thrown.</p>
 * <p>Also note that currently lock can only be obtained through <code>attempt</code>
 * api with specified timeout instead <code>acquire</code> is not supported.
 *
 * Internally, the upgrade is done through a Semaphore where a thread with
 * a higher priority will obtain the write lock first. The following scenarios then can
 * happen:
 * <ul>
 * <li>If there are multiple read locks granted (and no write lock request in waiting),
 *     an upgrade will release one read lock
 *     (decrease the counter), bump up upagrade counter, increase the current thread priority,
 *     set a thread local as upgrade thread,
 *     and place a write lock acquire() call. Upon waken up, it will check if the current
 *     thread is an upgrade. If it is, restore the thread priority, and decrease the
 *     upgrade counter.</li>
 * <li>If there are mutiple write locks request in waiting (and only one read lock granted),
 *     decrease the read lock counter,
 *     bump up the upgrade counter, and increase the current thread priority.
 *     When one of the writer gets wake up, it will first check
 *     if upgrade counter is zero. If not, it will first release the semaphore so the upgrade
 *     thread can grab it, check the semaphore is gone, do notify, and issue myself another
 *     acquire to grab the next available semaphore.</li>
 *  </ul>
 *
 * @author Ben Wang
*/
public class ReadWriteLockWithUpgrade implements ReadWriteLock
{
   private long                  activeReaders_ = 0;
   protected Thread              activeWriter_ = null;
   private long                  waitingReaders_ = 0;
   private long                  waitingWriters_ = 0;
   private long                  waitingUpgrader_ = 0;
   // Store a default object to signal that we are upgrade thread.
   //protected final ThreadLocal   upgraderLocal_ = new ThreadLocal();
   protected static final Map upgraderLocal_ = new ThreadLocalMap();
   protected static final Object dummy_ = new Object();
   protected final ReaderLock    readerLock_ = new ReaderLock();
   protected final WriterLock    writerLock_ = new WriterLock();
   protected static final Log log_=LogFactory.getLog(ReadWriteLockWithUpgrade.class);


   public String toString()
   {
      StringBuffer sb=new StringBuffer();
      sb.append("activeReaders=").append(activeReaders_).append(", activeWriter=").append(activeWriter_);
      sb.append(", waitingReaders=").append(waitingReaders_).append(", waitingWriters=").append(waitingWriters_);
      sb.append(", waitingUpgrader=").append(waitingUpgrader_);
      return sb.toString();
   }


   public Sync writeLock()
   {
      return writerLock_;
   }

   public Sync readLock()
   {
      return readerLock_;
   }

   /**
    * Attempt to obtain an upgrade to writer lock. If successful, the read lock is upgraded to
    * write lock. If fails, the owner retains the read lock.
    *
    * @param msecs Time to wait in millisecons.
    * @return Sync object. Null if not successful or timeout.
    */
   public Sync upgradeLockAttempt(long msecs) throws UpgradeException
   {
      if (activeReaders_ == 0)
         throw new RuntimeException("No reader lock available for upgrade");

      synchronized (writerLock_) {
         if(waitingUpgrader_ >=1) {
            String errStr="upgradeLockAttempt(): more than one reader trying to simultaneously upgrade to write lock";
            log_.error(errStr);
            throw new UpgradeException(errStr);
         }
         waitingUpgrader_++;
         upgraderLocal_.put(this,dummy_);
      }

      // If there is only one reader left, switch to write lock immediately.
      // If there is more than one reader, release this one and acquire the write
      // lock. There is still a chance for deadlock when there are two reader locks
      // and suddenly the second lock is released when this lock is released and acquired
      // as is else case. Solution is to let it timeout.
      if (activeReaders_ == 1) {
         resetWaitingUpgrader();
         return changeLock();
      } else {
         readerLock_.release();
         try {
            if (!writerLock_.attempt(msecs)) {
               log_.error("upgradeLock(): failed");
               resetWaitingUpgrader();

               if(!readerLock_.attempt(msecs)) {
                  String errStr="ReadWriteLockWithUpgrade.upgradeLockAttempt():" +
                        " failed to upgrade to write lock and also failed to re-obtain the read lock";
                  log_.error(errStr);
                  throw new IllegalStateException(errStr);
               }
               return null;
            }
            resetWaitingUpgrader();
         } catch (InterruptedException ex) {
            resetWaitingUpgrader();
            return null;
         }

         return writerLock_;
      }
   }

   private void resetWaitingUpgrader() {
      synchronized (writerLock_) {
         waitingUpgrader_--;
         upgraderLocal_.remove(this);
      }
   }

   protected synchronized Sync changeLock()
   {
      --activeReaders_;

      if (!startWrite()) {
         // Something is wrong.
         return null;
      }

      return writerLock_;
   }

   /*
     A bunch of small synchronized methods are needed
     to allow communication from the Lock objects
     back to this object, that serves as controller
   */
   protected synchronized void cancelledWaitingReader()
   {
      --waitingReaders_;
   }

   protected synchronized void cancelledWaitingWriter()
   {
      --waitingWriters_;
   }

   /**
    * Override this method to change to reader preference *
    */
   protected boolean allowReader()
   {
      return activeWriter_ == null && waitingWriters_ == 0 && waitingUpgrader_ == 0;
   }

   protected synchronized boolean startRead()
   {
      boolean allowRead = allowReader();
      if (allowRead) {
         ++activeReaders_;
      }
      return allowRead;
   }

   protected synchronized boolean startWrite()
   {
      // The allowWrite expression cannot be modified without
      // also changing startWrite, so is hard-wired
      boolean allowWrite = activeWriter_ == null && activeReaders_ == 0;
      if (allowWrite) activeWriter_ = Thread.currentThread();
      return allowWrite;
   }

   /*
      Each of these variants is needed to maintain atomicity of wait counts during wait loops. They could be
      made faster by manually inlining each other. We hope that compilers do this for us though.
   */
   protected synchronized boolean startReadFromNewReader()
   {
      boolean pass = startRead();
      if (!pass) ++waitingReaders_;
      return pass;
   }

   protected synchronized boolean startWriteFromNewWriter()
   {
      boolean pass = startWrite();
      if (!pass) ++waitingWriters_;
      return pass;
   }

   protected synchronized boolean startReadFromWaitingReader()
   {
      boolean pass = startRead();
      if (pass) --waitingReaders_;
      return pass;
   }

   protected synchronized boolean startWriteFromWaitingWriter()
   {
      boolean pass = startWrite();
      if (pass) --waitingWriters_;
      return pass;
   }

   /**
    * Called upon termination of a read.
    * Returns the object to signal to wake up a waiter, or null if no such
    */
   protected synchronized Signaller endRead()
   {
      if (activeReaders_ != 0 && --activeReaders_ == 0 && waitingWriters_ > 0)
         return writerLock_;
      else
         return null;
   }


   /**
    * Called upon termination of a write.
    * Returns the object to signal to wake up a waiter, or null if no such
    */
   protected synchronized Signaller endWrite()
   {
      activeWriter_ = null;
      if (waitingReaders_ > 0 && allowReader())
         return readerLock_;
      else if (waitingWriters_ > 0)
         return writerLock_;
      else
         return null;
   }


   /**
    * Reader and Writer requests are maintained in two different
    * wait sets, by two different objects. These objects do not
    * know whether the wait sets need notification since they
    * don't know preference rules. So, each supports a
    * method that can be selected by main controlling object
    * to perform the notifications.  This base class simplifies mechanics.
    */

   static interface Signaller
   { // base for ReaderLock and WriterLock
      void signalWaiters();
   }

   protected class ReaderLock implements Signaller, Sync
   {

      public void acquire() throws InterruptedException
      {
         throw new RuntimeException("acquire(): Operation currently not supported.");
      }

      public void release()
      {
         Signaller s = endRead();
         if (s != null) {
            s.signalWaiters();
         }
      }

      public synchronized void signalWaiters()
      {
         ReaderLock.this.notifyAll();
      }

      public boolean attempt(long msecs) throws InterruptedException
      {
         if (Thread.interrupted()) throw new InterruptedException();
         InterruptedException ie = null;
         synchronized (this) {
            if (msecs <= 0)
               return startRead();
            else if (startReadFromNewReader())
               return true;
            else {
               long waitTime = msecs;
               long start = System.currentTimeMillis();
               while(true) {
                  try {
                     ReaderLock.this.wait(waitTime);
                  }
                  catch(InterruptedException ex) {
                     cancelledWaitingReader();
                     ie=ex;
                     break;
                  }
                  if(startReadFromWaitingReader())
                     return true;
                  else {
                     waitTime=msecs - (System.currentTimeMillis() - start);
                     if(waitTime <= 0) {
                        cancelledWaitingReader();
                        break;
                     }
                  }
               }
            }
         }
         // safeguard on interrupt or timeout:
         writerLock_.signalWaiters();
         if (ie != null)
            throw ie;
         else
            return false; // timed out
      }

   }

   protected class WriterLock implements Signaller, Sync
   {

      public void acquire() throws InterruptedException
      {
         throw new RuntimeException("acquire(): Operation currently not supported.");
      }

      public void release()
      {
         Signaller s = endWrite();
         if (s != null) s.signalWaiters();
      }

      // Waking up all thread in waiting now for them to compete.
      // Thread with higher priority, i.e., upgrading, will win.
      public synchronized void signalWaiters()
      {
         WriterLock.this.notifyAll();
      }

      public boolean attempt(long msecs) throws InterruptedException
      {
         if (Thread.interrupted()) throw new InterruptedException();
         InterruptedException ie = null;

         synchronized (WriterLock.this) {
            if (msecs <= 0) {
               // Upgrade thread has prioirty.
               if (waitingUpgrader_ != 0) {
                  if (upgraderLocal_.get(ReadWriteLockWithUpgrade.this) != null) {
                     log_.info("attempt(): upgrade to write lock");
                     return startWrite();
                  }
                  else
                     return false;
               } else
                  return startWrite();
            } else if (startWriteFromNewWriter())
               return true;
            else {
               long waitTime = msecs;
               long start = System.currentTimeMillis();
               while(true) {
                  try {
                     WriterLock.this.wait(waitTime);
                  }
                  catch(InterruptedException ex) {
                     cancelledWaitingWriter();
                     WriterLock.this.notifyAll();
                     ie=ex;
                     break;
                  }

                  if(waitingUpgrader_ != 0) { // Has upgrade request
                     if(upgraderLocal_.get(ReadWriteLockWithUpgrade.this) != null) { // Upgrade thread
                        if(startWriteFromWaitingWriter())
                           return true;
                     }
                     else { // Normal write thread, go back to wait.
                        continue;
                     }
                  }
                  else { // Every one is normal write thread. Compete; if fail go back to wait.
                     if(startWriteFromWaitingWriter())
                        return true;
                  }

                  waitTime=msecs - (System.currentTimeMillis() - start);
                  if(waitTime <= 0) {
                     cancelledWaitingWriter();
                     WriterLock.this.notifyAll();
                     break;
                  }
               }
            }
         }

         readerLock_.signalWaiters();
         if (ie != null)
            throw ie;
         else
            return false; // timed out
      }

   }




}

