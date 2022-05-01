// $Id: UpgradeException.java 2073 2006-06-19 12:33:28Z  $

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on Jan 18, 2003
 */
package org.jboss.cache.lock;


/**
 * Used when a read-lock cannot be upgraded to a write-lock
 *
 * @author Bela Ban
 * @version $Revision: 2073 $
 */
public class UpgradeException extends LockingException
{

   /**
    * Constructor for UpgradeException.
    *
    * @param msg
    */
   public UpgradeException(String msg)
   {
      super(msg);
   }

   public UpgradeException(String msg, Throwable cause)
   {
      super(msg, cause);
   }

}
