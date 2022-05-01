package org.jboss.cache.optimistic;

import org.jboss.cache.CacheException;

/**
 * Denotes exceptions to do with data versioning in optimistic locking
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 1.4.1
 */
public class DataVersioningException extends CacheException
{

   public DataVersioningException()
   {
   }

   public DataVersioningException(String msg)
   {
      super(msg);
   }

   public DataVersioningException(String msg, Throwable cause)
   {
      super(msg, cause);
   }
}
