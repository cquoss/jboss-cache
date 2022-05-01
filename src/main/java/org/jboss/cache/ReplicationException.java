// $Id: ReplicationException.java 2710 2006-10-20 13:27:09Z msurtani $

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache;

/**
 * Thrown when a replication problem occurred
 */
public class ReplicationException extends CacheException {

   static final long serialVersionUID = -2141504272926290430L;

   public ReplicationException() {
      super();
   }

   public ReplicationException(String msg) {
      super(msg);
   }

   public ReplicationException(String msg, Throwable cause) {
      super(msg, cause);
   }

   public String toString() {
      return super.toString();
   }
}
