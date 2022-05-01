/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;
/**
 * @author Ben
 */
public class OwnerNotExistedException extends Exception
{

   /**
    *
    */
   public OwnerNotExistedException()
   {
      super();
   }

   /**
    * @param message
    */
   public OwnerNotExistedException(String message)
   {
      super(message);
   }

   /**
    * @param message
    * @param cause
    */
   public OwnerNotExistedException(String message, Throwable cause)
   {
      super(message, cause);
   }

   /**
    * @param cause
    */
   public OwnerNotExistedException(Throwable cause)
   {
      super(cause);
   }

}
