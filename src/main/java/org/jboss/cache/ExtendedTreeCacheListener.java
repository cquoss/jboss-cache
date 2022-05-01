/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache;

/**
 * Callbacks for various events regarding TreeCache. 
 * 
 * <p> This interface is an extension to TreeCacheListener to include newly added event 
 * types in release 1.2.4. We will merge it with TreeCacheListener in release 1.3. </p> 
 * 
 * <p> Implementers should avoid performing long running actions,
 * as this blocks the cache. If you have to do so, please start a new thread. </p>
 * 
 * @author Hany Mesha
 * @version $Revision: 1022 $
 */

public interface ExtendedTreeCacheListener
{
   /**
    * Called when a node is about to be evicted or has been evicted from the 
    * in-memory cache.
    * Note: Currently TreeCacheListener has {@link TreeCacheListener#nodeEvicted(Fqn)} 
    * which will be merged with method in release 1.3.
    * 
    * @param fqn
    * @param pre
    * @see TreeCacheListener#nodeEvicted(Fqn)
    */
   void nodeEvict(Fqn fqn, boolean pre);
   
   /**
    * Called when a node is about to be removed or has been removed from the 
    * in-memory cache.
    * Note: Currently TreeCacheListener has {@link TreeCacheListener#nodeRemoved(Fqn)} 
    * which will be merged with this method in release 1.3.
    * 
    * @param fqn
    * @param pre
    * @param isLocal
    * @see TreeCacheListener#nodeRemoved(Fqn)
    */
   void nodeRemove(Fqn fqn, boolean pre, boolean isLocal);

   /**
    * Called when a node is about to be modified or has been modified.  
    * Note: Currently TreeCacheListener has {@link TreeCacheListener#nodeModified(Fqn)} 
    * which will be merged with this method in release 1.3.
    * 
    * @param fqn
    * @param pre
    * @param isLocal
    * @see TreeCacheListener#nodeModified(Fqn)
    */ 
   void nodeModify(Fqn fqn, boolean pre, boolean isLocal);
   
   /**
    * Called when a node is to be or has been activated into memory via the 
    * CacheLoader that was evicted earlier.
    * 
    * @param fqn
    * @param pre
    */
   void nodeActivate(Fqn fqn, boolean pre);
    
   /**
    * Called when a node is to be or has been written to the backend store via the 
    * cache loader due to a node eviction by the eviction policy provider
    *  
    * @param fqn
    * @param pre
    */
   void nodePassivate(Fqn fqn, boolean pre);
}

