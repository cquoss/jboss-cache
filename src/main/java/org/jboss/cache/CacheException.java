// $Id: CacheException.java 1191 2006-02-13 16:22:08Z bela $

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache;

/**
 * CacheException, mother of all cache exceptions
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>.
 * @version $Revision: 1191 $
 *          <p/>
 *          <p><b>Revisions:</b>
 *          <p/>
 *          <p>Dec 27 2002 Bela Ban: first implementation
 *          <p>Jan 20 2003 Bela Ban: extend NestedException (otherwise build with JDK 1.3 fails)
 */
public class CacheException extends Exception {

   public CacheException() {
      super();
   }

   public CacheException(String msg) {
      super(msg);
   }

   public CacheException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
