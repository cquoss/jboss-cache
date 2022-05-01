/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;

import org.jboss.cache.lock.LockingException;
import org.jboss.cache.lock.TimeoutException;

/**
 * Represents a DataNode in the cache.
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 *
 */
public interface DataNode extends TreeNode
{

   /** Lock type of none. */
   int LOCK_TYPE_NONE = 0;

   /** Lock type of read. */
   int LOCK_TYPE_READ = 1;

   /** Lock type of write. */
   int LOCK_TYPE_WRITE = 2;

   public static final String REMOVAL_MARKER = "__JBOSS_MARKED_FOR_REMOVAL";

   /** Initialized property for debugging "print_lock_details" */
   boolean PRINT_LOCK_DETAILS = Boolean.getBoolean("print_lock_details");

   /**
    * Returns true if a lock is acquired.
    * @param lock_acquisition_timeout milliseconds to wait
    * @param lockTypeWrite lock type to use
    */
   boolean acquire(Object caller, long lock_acquisition_timeout, int lockTypeWrite)
      throws InterruptedException, LockingException, TimeoutException;


   /**
    * Returns a copy of this node.
    */
   Object clone() throws CloneNotSupportedException;

   boolean isMarkedForRemoval();

   void unmarkForRemoval(boolean deep);

   void markForRemoval();
}
