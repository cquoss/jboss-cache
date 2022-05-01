// $Id: DeadlockException.java 2073 2006-06-19 12:33:28Z  $

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on Jan 18, 2003
 */
package org.jboss.cache.lock;


/**
 * Used when a lock acquisition would cause a deadlock. This will only be used
 * once deadlock detection is in place.
 *
 * @author Bela Ban
 * @version $Revision: 2073 $
 */
public class DeadlockException extends LockingException
{

   /**
    * Constructor for DeadlockException.
    *
    * @param msg
    */
   public DeadlockException(String msg)
   {
      super(msg);
   }

   public DeadlockException(String msg, Throwable cause)
   {
      super(msg, cause);
   }


}
