// $Id: NodeNotExistsException.java 2073 2006-06-19 12:33:28Z  $

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache;




/**
 * Thrown when an operation is attempted on a non-existing node in the cache
 *
 * @author <a href="mailto:bela@jboss.com">Bela Ban</a>.
 * @version $Id: NodeNotExistsException.java 2073 2006-06-19 12:33:28Z  $
 */

public class NodeNotExistsException extends CacheException {

   public NodeNotExistsException() {
      super();
   }


   public NodeNotExistsException(String msg) {
      super(msg);
   }


   public NodeNotExistsException(String msg, Throwable cause) {
      super(msg, cause);
   }



}
