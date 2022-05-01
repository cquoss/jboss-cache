package org.jboss.cache;

/**
 * Thrown when a member is suspected during remote method invocation
 * @author Bela Ban
 * @version $Id: SuspectException.java 1181 2006-02-13 08:40:05Z bela $
 */
public class SuspectException extends CacheException {

   public SuspectException() {
      super();
   }

   public SuspectException(String msg) {
      super(msg);
   }

   public SuspectException(String msg, Throwable cause) {
      super(msg, cause);
   }

   public String toString() {
      return super.toString();
   }
}
