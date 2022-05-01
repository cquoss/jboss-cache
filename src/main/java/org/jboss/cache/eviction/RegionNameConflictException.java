package org.jboss.cache.eviction;
/**
 * Region name conflicts with pre-existing regions.
 *
 * @author Ben Wang 2-2004
 */
public class RegionNameConflictException extends Exception
{
   public RegionNameConflictException() {
      super();
   }

   public RegionNameConflictException(String msg) {
      super(msg);
   }

   public RegionNameConflictException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
