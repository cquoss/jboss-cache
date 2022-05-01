package org.jboss.cache.eviction;

/**
 * @author Ben Wang, Feb 11, 2004
 */
public class EvictionException extends Exception
{
   public EvictionException()
   {
      super();
   }

   public EvictionException(String msg)
   {
      super(msg);
   }

   public EvictionException(String msg, Throwable cause)
   {
      super(msg, cause);
   }

}
