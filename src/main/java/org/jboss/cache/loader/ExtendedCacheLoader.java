package org.jboss.cache.loader;

import org.jboss.cache.Fqn;
import org.jboss.cache.marshall.RegionManager;

/**
 * Extends the {@link CacheLoader} interface by adding methods to support
 * serialized transfer of a portion of a cache tree.
 * <p>
 * <strong>NOTE:</strong> The methods in this interface will be merged into
 * <code>CacheLoader</code> in JBossCache 1.3.
 * </p>
 *             
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public interface ExtendedCacheLoader extends CacheLoader
{
   /**
    * Fetch a portion of the state for this cache from secondary storage 
    * (disk, DB) and return it as a byte buffer.
    * This is for activation of a portion of new cache from a remote cache. 
    * The new cache would then call {@link #storeState(byte[], Fqn)}.
    * 
    * @param subtree Fqn naming the root (i.e. highest level parent) node of
    *                the subtree for which state is requested.
    *                
    * @see org.jboss.cache.TreeCache#activateRegion(String)
    */
   byte[] loadState(Fqn subtree) throws Exception;
   
   /**
    * Store the given portion of the cache tree's state in secondary storage. 
    * Overwrite whatever is currently in secondary storage.  If the transferred 
    * state has Fqns equal to or children of parameter <code>subtree</code>, 
    * then no special behavior is required.  Otherwise, ensure that
    * the state is integrated under the given <code>subtree</code>. Typically
    * in the latter case <code>subtree</code> would be the Fqn of the buddy 
    * backup region for
    * a buddy group; e.g.
    * <p>
    * If the the transferred state had Fqns starting with "/a" and
    * <code>subtree</code> was "/_BUDDY_BACKUP_/192.168.1.2:5555" then the
    * state should be stored in the local persistent store under
    * "/_BUDDY_BACKUP_/192.168.1.2:5555/a"
    * </p>
    * 
    * @param state   the state to store
    * @param subtree Fqn naming the root (i.e. highest level parent) node of
    *                the subtree included in <code>state</code>.  If the Fqns  
    *                of the data included in <code>state</code> are not 
    *                already children of <code>subtree</code>, then their
    *                Fqns should be altered to make them children of 
    *                <code>subtree</code> before they are persisted.
    */   
   void storeState(byte[] state, Fqn subtree) throws Exception;
   
   /**
    * Sets the {@link RegionManager} this object should use to manage 
    * marshalling/unmarshalling of different regions using different
    * classloaders.
    * <p>
    * <strong>NOTE:</strong> This method is only intended to be used
    * by the <code>TreeCache</code> instance this cache loader is
    * associated with.
    * </p>
    * 
    * @param manager    the region manager to use, or <code>null</code>.
    */
   void setRegionManager(RegionManager manager);

}
