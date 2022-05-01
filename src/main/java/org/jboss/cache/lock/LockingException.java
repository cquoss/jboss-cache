// $Id: LockingException.java 2073 2006-06-19 12:33:28Z  $

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.lock;

import org.jboss.cache.CacheException;

import java.util.Map;


/**
 * Used for all locking-related exceptions, e.g. when  a lock could not be
 * acquired within the timeout, or when a deadlock was detected.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>.
 * @version $Revision: 2073 $
 *          <p/>
 *          <p><b>Revisions:</b>
 *          <p/>
 *          <p>Dec 27 2002 Bela Ban: first implementation
 */

public class LockingException extends CacheException
{

   /**
    * A list of all nodes that failed to acquire a lock
    */
   Map failed_lockers = null;

   public LockingException()
   {
      super();
   }

   public LockingException(Map failed_lockers)
   {
      super();
      this.failed_lockers = failed_lockers;
   }

   public LockingException(String msg)
   {
      super(msg);
   }

   public LockingException(String msg, Map failed_lockers)
   {
      super(msg);
      this.failed_lockers = failed_lockers;
   }

   public LockingException(String msg, Throwable cause)
   {
      super(msg, cause);
   }

   public LockingException(String msg, Throwable cause, Map failed_lockers)
   {
      super(msg, cause);
      this.failed_lockers = failed_lockers;
   }

   public String toString()
   {
      String retval = super.toString();
      if (failed_lockers != null && failed_lockers.size() > 0)
         retval = retval + ", failed lockers: " + failed_lockers;
      return retval;
   }

}
