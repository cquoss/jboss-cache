/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache;


import org.jgroups.View;

/**
 * Callbacks for various events regarding TreeCache. Implementers should avoid performing long running actions,
 * as this blocks the cache. If you have to do so, please start a new thread.
 * @author Bela Ban
 * @author Ben Wang
 * @version $Revision: 445 $
 */
public interface TreeCacheListener {
   /**
    * Called when a node is created
    * @param fqn
    */
   void nodeCreated(Fqn fqn);

   /**
    * Called when a node is removed.
    * @param fqn
    */
   void nodeRemoved(Fqn fqn);

   /**
    * Called when a node is loaded into memory via the CacheLoader. This is not the same
    * as {@link #nodeCreated(Fqn)}.
    */
   void nodeLoaded(Fqn fqn);

   /**
    * Called when a node is evicted (not the same as remove()).
    * @param fqn
    */
   void nodeEvicted(Fqn fqn);

   /**
    * Called when a node is modified, e.g., one (key, value) pair
    * in the internal map storage has been modified.
    * @param fqn
    */
   void nodeModified(Fqn fqn);

   /**
    * Called when a node is visisted, i.e., get().
    * @param fqn
    */
   void nodeVisited(Fqn fqn);

   /**
    * Called when the cache is started.
    * @param cache
    */
   void cacheStarted(TreeCache cache);

   /**
    * Called when the cache is stopped.
    * @param cache
    */
   void cacheStopped(TreeCache cache);

   void viewChange(View new_view);  // might be MergeView after merging
}
