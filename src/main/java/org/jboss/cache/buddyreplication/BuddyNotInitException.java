package org.jboss.cache.buddyreplication;

import org.jboss.cache.CacheException;

/**
 * Exception to depict that a buddy has not been initialised to participate in any comms
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 1.4.1
 */
public class BuddyNotInitException extends CacheException
{

   public BuddyNotInitException()
   {
   }

   public BuddyNotInitException(String msg)
   {
      super(msg);
   }

   public BuddyNotInitException(String msg, Throwable cause)
   {
      super(msg, cause);
   }
}
