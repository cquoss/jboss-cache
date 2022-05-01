/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache;

/**
 * Thrown when an attempt is made to 
 * {@link TreeCache#activateRegion(String) activate a subtree} 
 * roote in  Fqn that already has an existing node in the cache.
 * 
 * @see TreeCache#activateRegion(String)
 * @see TreeCache#exists(Fqn)
 * @see TreeCache#exists(String)
 * 
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public class RegionNotEmptyException extends CacheException
{

   /** The serialVersionUID */
   private static final long serialVersionUID = 1L;

   public RegionNotEmptyException()
   {
      super();
   }

   public RegionNotEmptyException(String msg)
   {
      super(msg);
   }

   public RegionNotEmptyException(String msg, Throwable cause)
   {
      super(msg, cause);
   }

}
