/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.loader;


/**
 * A <code>FileCacheLoader</code> that implements 
 * <code>ExtendedCacheLoader</code>.
 * 
 * @deprecated {@link org.jboss.cache.loader.FileCacheLoader} now implements
 *             {@link org.jboss.cache.loader.ExtendedCacheLoader} so this
 *             class adds nothing to it and will be removed in 2.0.
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Id$
 */
public class FileExtendedCacheLoader extends FileCacheLoader 
{
// -----------------------------------------------------------  Constructors

   /**
    * Create a new FileExtendedCacheLoader.
    * 
    */
   public FileExtendedCacheLoader()
   {
      super();
   }

}
