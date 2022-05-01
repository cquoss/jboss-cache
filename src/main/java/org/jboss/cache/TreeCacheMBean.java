/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;

import org.jboss.cache.config.Option;
import org.jboss.cache.loader.CacheLoader;
import org.jboss.cache.marshall.RegionNameConflictException;
import org.jboss.cache.marshall.RegionNotFoundException;
import org.jboss.cache.marshall.TreeCacheMarshaller;
import org.jboss.system.ServiceMBean;
import org.w3c.dom.Element;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * MBean interface.
 *
 * @author Bela Ban
 * @author Ben Wang
 * @version $Id: TreeCacheMBean.java 5188 2008-01-22 10:59:59Z manik.surtani@jboss.com $
 */
public interface TreeCacheMBean extends ServiceMBean
{

   Object getLocalAddress();

   Vector getMembers();

   boolean isCoordinator();

   /**
    * Get the name of the replication group
    */
   String getClusterName();

   String getVersion();

   /**
    * Set the name of the replication group
    */
   void setClusterName(String name);


    /**
     * Sets whether a {@link TreeCacheMarshaller} instance should be created
     * to manage different classloaders to use for unmarshalling replicated
     * objects.
     * <p/>
     * This property must be set to <code>true</code> before any call to
     * {@link #registerClassLoader(String, ClassLoader)} or
     * {@link #activateRegion(String)}
     * </p>
     *
     * TreeCacheMarshaller is ALWAYS used.  This is now synonymous with setUseRegionBasedMarshalling()
     *
     * @deprecated
     */
    void setUseMarshalling(boolean isTrue);

    /**
     * Gets whether a {@link TreeCacheMarshaller} instance should be used
     * to manage different classloaders to use for unmarshalling replicated
     * objects.
     *
     * TreeCacheMarshaller is ALWAYS used.  This is now synonymous with getUseRegionBasedMarshalling()
     * @deprecated
     */
    boolean getUseMarshalling();

    /**
     * Sets whether marshalling uses scoped class loaders on a per region basis.
     * <p />
     * This property must be set to <code>true</code> before any call to
     * {@link #registerClassLoader(String, ClassLoader)} or
     * {@link #activateRegion(String)}

     * @param isTrue
     */
    void setUseRegionBasedMarshalling(boolean isTrue);

    /**
     * Tests whether region based marshaling s used.
     * @return true if region based marshalling is used.
     */
    boolean getUseRegionBasedMarshalling();


    /**
     * Gets whether the cache should register its interceptor mbeans.
     * These mbeans are used to capture and publish interceptor statistics.
     *
     * @return true if mbeans should be registered for each interceptor
     */
    boolean getUseInterceptorMbeans();

   boolean isUsingEviction();

    /**
     * Registers the given classloader with <code>TreeCacheMarshaller</code> for
     * use in unmarshalling replicated objects for the specified region.
     *
     * @param fqn The fqn region. This fqn and its children will use this classloader for (un)marshalling.
     * @param cl  The class loader to use
     * @throws RegionNameConflictException if <code>fqn</code> is a descendant of
     *                                     an FQN that already has a classloader
     *                                     registered.
     * @throws IllegalStateException       if <code>useMarshalling</code> is <code>false</code>
     */
    void registerClassLoader(String fqn, ClassLoader cl) throws RegionNameConflictException;

    /**
     * Instructs the <code>TreeCacheMarshaller</code> to no longer use a special
     * classloader to unmarshal replicated objects for the specified region.
     *
     * @param fqn The fqn of the root node of region.
     * @throws RegionNotFoundException if no classloader has been registered for
     *                                 <code>fqn</code>.
     * @throws IllegalStateException   if <code>useMarshalling</code> is <code>false</code>
     */
    void unregisterClassLoader(String fqn) throws RegionNotFoundException;

    /**
     * Get the cluster properties (e.g. the protocol stack specification in case of JGroups)
     */
    String getClusterProperties();

    /**
     * Set the cluster properties. If the cache is to use the new properties, it has to be redeployed
     *
     * @param cluster_props The properties for the cluster (JGroups)
     */
    void setClusterProperties(String cluster_props);

    /**
     * Dumps the contents of the TransactionTable
     */
    String dumpTransactionTable();

    String getInterceptorChain();

    List getInterceptors();

    Element getCacheLoaderConfiguration();

    void setCacheLoaderConfiguration(Element cache_loader_config);

    CacheLoader getCacheLoader();

    boolean getSyncCommitPhase();

    void setSyncCommitPhase(boolean sync_commit_phase);

    boolean getSyncRollbackPhase();

    void setSyncRollbackPhase(boolean sync_rollback_phase);

    /**
     * Setup eviction policy configuration
     */
    void setEvictionPolicyConfig(org.w3c.dom.Element config);

    /**
     * Convert a list of elements to the JG property string
     */
    void setClusterConfig(org.w3c.dom.Element config);

    /**
     * Get the max time to wait until the initial state is retrieved. This is used in a replicating cache: when a new cache joins the cluster, it needs to acquire the (replicated) state of the other members to initialize itself. If no state has been received within <tt>timeout</tt> milliseconds, the map will be empty.
     *
     * @return long Number of milliseconds to wait for the state. 0 means to wait forever.
     */
    long getInitialStateRetrievalTimeout();

    /**
     * Set the initial state transfer timeout (see {@link #getInitialStateRetrievalTimeout()})
     */
    void setInitialStateRetrievalTimeout(long timeout);

    /**
     * Returns the current caching mode. Valid values are <ul> <li>LOCAL <li>REPL_ASYNC <li>REPL_SYNC <ul>
     *
     * @return String The caching mode
     */
    String getCacheMode();

    /**
     * Sets the default caching mode)
     */
    void setCacheMode(String mode) throws Exception;

    /**
     * Returns the default max timeout after which synchronous replication calls return.
     *
     * @return long Number of milliseconds after which a sync repl call must return. 0 means to wait forever
     */
    long getSyncReplTimeout();

    /**
     * Sets the default maximum wait time for synchronous replication to receive all results
     */
    void setSyncReplTimeout(long timeout);

    boolean getUseReplQueue();

    void setUseReplQueue(boolean flag);

    long getReplQueueInterval();

    void setReplQueueInterval(long interval);

    int getReplQueueMaxElements();

    void setReplQueueMaxElements(int max_elements);

    void setPojoCacheConfig(Element config) throws CacheException;

    Element getPojoCacheConfig();

    /**
     * Returns the transaction isolation level.
     */
    String getIsolationLevel();

    /**
     * Set the transaction isolation level. This determines the locking strategy to be used
     */
    void setIsolationLevel(String level);

    /**
     * Gets whether inserting or removing a node requires a write lock
     * on the node's parent (when pessimistic locking is used.)
     * <p/>
     * The default value is <code>false</code>
     */
    boolean getLockParentForChildInsertRemove();

    /**
     * Sets whether inserting or removing a node requires a write lock
     * on the node's parent (when pessimistic locking is used.)
     * <p/>
     * The default value is <code>false</code>
     */
    void setLockParentForChildInsertRemove(boolean lockParentForChildInsertRemove);
    
    /**
     * Returns whether or not on startup the initial state will be acquired
     * from existing members.
     *
     * @return <code>true</code> if {@link #isInactiveOnStartup()} is
     *         <code>false</code>, buddy replication is not configured and 
     *         either {@link #getFetchInMemoryState()} is <code>true</code> 
     *         or a cache loader's <code>FetchPersistentState</code> property 
     *         is <code>true</code>.
     */
    boolean getFetchStateOnStartup();

    /**
     * Returns whether or not any initial state transfer or subsequent partial
     * state transfer following an <code>activateRegion</code> call should
     * include in-memory state. Allows for warm/hot caches (true/false). The
     * characteristics of a state transfer can be further defined by a cache
     * loader's FetchPersistentState property.
     */
    boolean getFetchInMemoryState();

    /**
     * Non-functional method maintained for compile time compatibility.
     *
     * @deprecated will just log a warning; use setFetchInMemoryState and
     *             any cache loader's setFetchPersistentState property
     */
    void setFetchStateOnStartup(boolean flag);

    /**
     * Sets whether or not any initial or subsequent partial state transfer
     * should include in-memory state.
     */
    void setFetchInMemoryState(boolean flag);

    /**
     * Gets the format version of the data transferred during an initial state
     * transfer or a call to {@link #activateRegion(String)}.  Different
     * releases of JBossCache may format this data differently; this property
     * identifies the format version being used by this cache instance.
     * <p>
     * The default value for this property is
     * {@link TreeCache#DEFAULT_REPLICATION_VERSION}.
     * </p>
     *
     * @return    a short identifying JBossCache release; e.g. <code>124</code>
     *            for JBossCache 1.2.4
     *
     * @deprecated use {@link #getReplicationVersion()} instead
     */
    short getStateTransferVersion();

    /**
     * Sets the format version of the data transferred during an initial state
     * transfer or a call to {@link #activateRegion(String)}.  Different
     * releases of JBossCache may format this data differently; this property
     * identifies the format version being used by this cache instance. Setting
     * this property to a value other than the default allows a cache instance
     * from a later release to interoperate with a cache instance from an
     * earlier release.
     *
     * @param version a short identifying JBossCache release;
     *                e.g. <code>124</code> for JBossCache 1.2.4
     *
     * @deprecated use {@link #setReplicationVersion(String)} instead
     */
    void setStateTransferVersion(short version);

    /**
     * Gets the format version of the data transferred during an initial state
     * transfer or a call to {@link #activateRegion(String)}.  Different
     * releases of JBossCache may format this data differently; this property
     * identifies the format version being used by this cache instance.
     * <p>
     * The default value for this property is
     * {@link TreeCache#DEFAULT_REPLICATION_VERSION}.
     * </p>
     *
     * @return    a short identifying JBossCache release; e.g. <code>124</code>
     *            for JBossCache 1.2.4
     */
    String getReplicationVersion();

    /**
     * Sets the format version of the data transferred during an initial state
     * transfer or a call to {@link #activateRegion(String)}.  Different
     * releases of JBossCache may format this data differently; this property
     * identifies the format version being used by this cache instance. Setting
     * this property to a value other than the default allows a cache instance
     * from a later release to interoperate with a cache instance from an
     * earlier release.
     *
     * @param version a short identifying JBossCache release;
     *                e.g. <code>124</code> for JBossCache 1.2.4
     */
    void setReplicationVersion(String version);

    /**
     * Default max time to wait for a lock. If the lock cannot be acquired within this time, a LockingException will be thrown.
     *
     * @return long Max number of milliseconds to wait for a lock to be acquired
     */
    long getLockAcquisitionTimeout();

    /**
     * Set the max time for lock acquisition. A value of 0 means to wait forever (not recomended). Note that lock acquisition timeouts may be removed in the future when we have deadlock detection.
     *
     * @param timeout
     */
    void setLockAcquisitionTimeout(long timeout);

    boolean getDeadlockDetection();

    void setDeadlockDetection(boolean dt);

    /**
     * Returns the name of the cache eviction policy (must be an implementation of EvictionPolicy)
     *
     * @return Fully qualified name of a class implementing the EvictionPolicy interface
     */
    String getEvictionPolicyClass();

    /**
     * Sets the classname of the eviction policy
     */
    void setEvictionPolicyClass(String eviction_policy_class);

    /**
     * Obtain eviction thread (if any) wake up interval in seconds
     */
    int getEvictionThreadWakeupIntervalSeconds();

    /**
     * Sets the TransactionManagerLookup object
     *
     * @param l
     */
    void setTransactionManagerLookup(TransactionManagerLookup l);

    String getTransactionManagerLookupClass();

    /**
     * Sets the class of the TransactionManagerLookup impl. This will attempt to create an instance, and will throw an exception if this fails.
     *
     * @param cl
     * @throws Exception
     */
    void setTransactionManagerLookupClass(String cl) throws Exception;

    javax.transaction.TransactionManager getTransactionManager();

    /**
     * Returns a reference to the TreeCache object itself.  Note that acquiring
     * such a reference in a JMX managed environment causes tight coupling
     * between the client using the cache and the cache service, thus negating
     * one of the key benefits of using JMX.
     */
    TreeCache getInstance();

    /**
     * Fetch the group state from the current coordinator. If successful, this will trigger setState().
     */
    void fetchState(long timeout) throws org.jgroups.ChannelClosedException, org.jgroups.ChannelNotConnectedException;

    void addTreeCacheListener(TreeCacheListener listener);

    void removeTreeCacheListener(TreeCacheListener listener);

    void createService() throws Exception;

    void destroyService();

    void startService() throws Exception;

    /**
     * Loads the indicated Fqn, plus all parents recursively from the CacheLoader. If no CacheLoader is present, this is a no-op
     *
     * @param fqn
     * @throws Exception
     */
    void load(String fqn) throws Exception;

    void stopService();

    Set getKeys(String fqn) throws CacheException;

    Set getKeys(Fqn fqn) throws CacheException;

    /**
     * Finds a node given its name and returns the value associated with a given key in its <code>data</code> map. Returns null if the node was not found in the tree or the key was not found in the hashmap.
     *
     * @param fqn The fully qualified name of the node.
     * @param key The key.
     */
    Object get(String fqn, Object key) throws CacheException;

    /**
     * Finds a node given its name and returns the value associated with a given key in its <code>data</code> map. Returns null if the node was not found in the tree or the key was not found in the hashmap.
     *
     * @param fqn The fully qualified name of the node.
     * @param key The key.
     */
    Object get(Fqn fqn, Object key) throws CacheException;


    Node get(String fqn) throws CacheException;

    Node get(Fqn fqn) throws CacheException;

    /**
     * Checks whether a given node exists in the tree
     *
     * @param fqn The fully qualified name of the node
     * @return boolean Whether or not the node exists
     */
    boolean exists(String fqn);

    /**
     * Checks whether a given node exists in the tree. Does not acquire any locks in doing so (result may be dirty read)
     *
     * @param fqn The fully qualified name of the node
     * @return boolean Whether or not the node exists
     */
    boolean exists(Fqn fqn);

    boolean exists(String fqn, Object key);

    /**
     * Checks whether a given key exists in the given node. Does not interact with CacheLoader, so the behavior is different from {@link #get(Fqn,Object)}
     *
     * @param fqn The fully qualified name of the node
     * @param key
     * @return boolean Whether or not the node exists
     */
    boolean exists(Fqn fqn, Object key);

    /**
     * Adds a new node to the tree and sets its data. If the node doesn not yet exist, it will be created. Also, parent nodes will be created if not existent. If the node already has data, then the new data will override the old one. If the node already existed, a nodeModified() notification will be generated. Otherwise a nodeCreated() motification will be emitted.
     *
     * @param fqn  The fully qualified name of the new node
     * @param data The new data. May be null if no data should be set in the node.
     */
    void put(String fqn, Map data) throws CacheException;

    /**
     * Adds a new node to the tree and sets its data. If the node doesn not yet exist, it will be created. Also, parent nodes will be created if not existent. If the node already has data, then the new data will override the old one. If the node already existed, a nodeModified() notification will be generated. Otherwise a nodeCreated() motification will be emitted.
     *
     * @param fqn  The fully qualified name of the new node
     * @param data The new data. May be null if no data should be set in the node.
     */
    void put(Fqn fqn, Map data) throws CacheException;

    /**
     * Adds a key and value to a given node. If the node doesn't exist, it will be created. If the node already existed, a nodeModified() notification will be generated. Otherwise a nodeCreated() motification will be emitted.
     *
     * @param fqn   The fully qualified name of the node
     * @param key   The key
     * @param value The value
     * @return Object The previous value (if any), if node was present
     */
    Object put(String fqn, Object key, Object value) throws CacheException;

    /**
     * Adds a key and value to a given node. If the node doesn't exist, it will be created. If the node already existed, a nodeModified() notification will be generated. Otherwise a nodeCreated() motification will be emitted.
     *
     * @param fqn   The fully qualified name of the node
     * @param key   The key
     * @param value The value
     * @return Object The previous value (if any), if node was present
     */
    Object put(Fqn fqn, Object key, Object value) throws CacheException;

    /**
     * Removes the node from the tree.
     *
     * @param fqn The fully qualified name of the node.
     */
    void remove(String fqn) throws CacheException;

    /**
     * Removes the node from the tree.
     *
     * @param fqn The fully qualified name of the node.
     */
    void remove(Fqn fqn) throws CacheException;

    /**
     * Called by eviction policy provider. Note that eviction is done only in local mode, that is, it doesn't replicate the node removal. This is will cause the replcation nodes not synchronizing, but it is ok since user is supposed to add the node again when get is null. After that, the contents will be in sync.
     *
     * @param fqn Will remove everythign assoicated with this fqn.
     * @throws CacheException
     */
    void evict(Fqn fqn) throws CacheException;

    /**
     * Removes <code>key</code> from the node's hashmap
     *
     * @param fqn The fullly qualified name of the node
     * @param key The key to be removed
     * @return The previous value, or null if none was associated with the given key
     */
    Object remove(String fqn, Object key) throws CacheException;

    /**
     * Removes <code>key</code> from the node's hashmap
     *
     * @param fqn The fullly qualified name of the node
     * @param key The key to be removed
     * @return The previous value, or null if none was associated with the given key
     */
    Object remove(Fqn fqn, Object key) throws CacheException;

    void removeData(String fqn) throws CacheException;

    void removeData(Fqn fqn) throws CacheException;

    /**
     * Force-releases all locks in this node and the entire subtree
     *
     * @param fqn
     */
    void releaseAllLocks(String fqn);

    /**
     * Force-releases all locks in this node and the entire subtree
     *
     * @param fqn
     */
    void releaseAllLocks(Fqn fqn);

    /**
     * Prints a representation of the node defined by <code>fqn</code>. Output includes name, fqn and data.
     */
    String print(String fqn);

    /**
     * Prints a representation of the node defined by <code>fqn</code>. Output includes name, fqn and data.
     */
    String print(Fqn fqn);

    /**
     * Returns all children of a given node
     *
     * @param fqn The fully qualified name of the node
     * @return Set A list of child names (as Strings)
     */
    Set getChildrenNames(String fqn) throws CacheException;

    /**
     * Returns all children of a given node
     *
     * @param fqn The fully qualified name of the node
     * @return Set A list of child names (as Objects). Must <em>not</em> be modified because this would modify the underlying node directly (will throw an exception if modification is attempted). Returns null of the parent node was not found, or if there are no children
     */
    Set getChildrenNames(Fqn fqn) throws CacheException;

    String toString();

    String printDetails();

    String printLockInfo();

    /**
     * Gets the number of read or write locks held across the entire tree
     */
    int getNumberOfLocksHeld();

    /**
     * Returns an <em>approximation</em> of the total number of nodes in the tree. Since this method doesn't acquire any locks, the number might be incorrect, or the method might even throw a ConcurrentModificationException
     */
    int getNumberOfNodes();

    /**
     * Returns an <em>approximation</em> of the total number of attributes in the tree. Since this method doesn't acquire any locks, the number might be incorrect, or the method might even throw a ConcurrentModificationException
     */
    int getNumberOfAttributes();

    List callRemoteMethods(Vector members, Method method, Object[] args, boolean synchronous, boolean exclude_self, long timeout) throws Exception;

    List callRemoteMethods(Vector members, String method_name, Class[] types, Object[] args, boolean synchronous, boolean exclude_self, long timeout) throws Exception;

    /**
     * Does the real work. Needs to acquire locks if accessing nodes, depending on the value of <tt>locking</tt>. If run inside a transaction, needs to (a) add newly acquired locks to {@link TransactionEntry}'s lock list, (b) add nodes that were created to {@link TransactionEntry}'s node list and (c) create {@link Modification}s and add them to {@link TransactionEntry}'s modification list and (d) create compensating modifications to undo the changes in case of a rollback
     *
     * @param fqn
     * @param data
     * @param create_undo_ops If true, undo operations will be created (default is true). Otherwise they will not be created (used by rollback()).
     */
    void _put(GlobalTransaction tx, String fqn, Map data, boolean create_undo_ops) throws CacheException;

    /**
     * Does the real work. Needs to acquire locks if accessing nodes, depending on the value of <tt>locking</tt>. If run inside a transaction, needs to (a) add newly acquired locks to {@link TransactionEntry}'s lock list, (b) add nodes that were created to {@link TransactionEntry}'s node list and (c) create {@link Modification}s and add them to {@link TransactionEntry}'s modification list and (d) create compensating modifications to undo the changes in case of a rollback
     *
     * @param fqn
     * @param data
     * @param create_undo_ops If true, undo operations will be created (default is true). Otherwise they will not be created (used by rollback()).
     */
    void _put(GlobalTransaction tx, Fqn fqn, Map data, boolean create_undo_ops) throws CacheException;

    /**
     * Does the real work. Needs to acquire locks if accessing nodes, depending on the value of <tt>locking</tt>. If run inside a transaction, needs to (a) add newly acquired locks to {@link TransactionEntry}'s lock list, (b) add nodes that were created to {@link TransactionEntry}'s node list and (c) create {@link Modification}s and add them to {@link TransactionEntry}'s modification list and (d) create compensating modifications to undo the changes in case of a rollback
     *
     * @param fqn
     * @param data
     * @param create_undo_ops If true, undo operations will be created (default is true).
     * @param erase_contents  Clear the existing hashmap before putting the new data into it Otherwise they will not be created (used by rollback()).
     */
    void _put(GlobalTransaction tx, Fqn fqn, Map data, boolean create_undo_ops, boolean erase_contents) throws CacheException;

    Object _put(GlobalTransaction tx, String fqn, Object key, Object value, boolean create_undo_ops) throws CacheException;

    Object _put(GlobalTransaction tx, Fqn fqn, Object key, Object value, boolean create_undo_ops) throws CacheException;

    void _remove(GlobalTransaction tx, String fqn, boolean create_undo_ops) throws CacheException;

    void _remove(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops) throws CacheException;

    Object _remove(GlobalTransaction tx, String fqn, Object key, boolean create_undo_ops) throws CacheException;

    Object _remove(GlobalTransaction tx, Fqn fqn, Object key, boolean create_undo_ops) throws CacheException;

    void setNodeLockingScheme(String nodeLockingScheme);

    String getNodeLockingScheme();

    /**
     * Causes the cache to transfer state for the subtree rooted at
     * <code>subtreeFqn</code> and to begin accepting replication messages
     * for that subtree.
     * <p/>
     * <strong>NOTE:</strong> This method will cause the creation of a node
     * in the local tree at <code>subtreeFqn</code> whether or not that
     * node exists anywhere else in the cluster.  If the node does not exist
     * elsewhere, the local node will be empty.  The creation of this node will
     * not be replicated.
     *
     * @param subtreeFqn Fqn string indicating the uppermost node in the
     *                   portion of the tree that should be activated.
     * @throws RegionNotEmptyException if the node <code>subtreeFqn</code>
     *                                 exists and has either data or children
     * @throws IllegalStateException   if {@link #getUseMarshalling() useMarshalling} is <code>false</code>
     */
    void activateRegion(String subtreeFqn)
            throws RegionNotEmptyException, RegionNameConflictException, CacheException;

    /**
     * Causes the cache to stop accepting replication events for the subtree
     * rooted at <code>subtreeFqn</code> and evict all nodes in that subtree.
     *
     * @param subtreeFqn Fqn string indicating the uppermost node in the
     *                   portion of the tree that should be activated.
     * @throws RegionNameConflictException if <code>subtreeFqn</code> indicates
     *                                     a node that is part of another
     *                                     subtree that is being specially
     *                                     managed (either by activate/inactiveRegion()
     *                                     or by registerClassLoader())
     * @throws CacheException              if there is a problem evicting nodes
     * @throws IllegalStateException       if {@link #getUseMarshalling() useMarshalling} is <code>false</code>
     */
    void inactivateRegion(String subtreeFqn) throws RegionNameConflictException, CacheException;

    /**
     * Gets whether the entire tree is inactive upon startup, only responding
     * to replication messages after {@link #activateRegion(String)} is
     * called to activate one or more parts of the tree.
     * <p/>
     * This property is only relevant if {@link #getUseMarshalling()} is
     * <code>true</code>.
     */
    boolean isInactiveOnStartup();

    /**
     * Sets whether the entire tree is inactive upon startup, only responding
     * to replication messages after {@link #activateRegion(String)} is
     * called to activate one or more parts of the tree.
     * <p>
     * This property is only relevant if {@link #getUseMarshalling()} is
     * <code>true</code>.
     *
     */
    void setInactiveOnStartup(boolean inactiveOnStartup);

    // ---------- OPTION overloaded methods --------------

    /**
     * The same as calling get(Fqn) except that you can pass in options for this specific method invocation.
     * {@link Option}
     *
     * @param fqn
     * @param option
     * @return
     * @throws CacheException
     */
    DataNode get(Fqn fqn, Option option) throws CacheException;

    /**
     * The same as calling get(Fqn, Object) except that you can pass in options for this specific method invocation.
     * {@link Option}
     *
     * @param fqn
     * @param option
     * @return
     * @throws CacheException
     */
    Object get(Fqn fqn, Object key, Option option) throws CacheException;

    /**
     * The same as calling get(Fqn, Object, boolean) except that you can pass in options for this specific method invocation.
     * {@link Option}
     *
     * @param fqn
     * @param option
     * @return
     * @throws CacheException
     */
    Object get(Fqn fqn, Object key, boolean sendNodeEvent, Option option) throws CacheException;

    /**
     * The same as calling remove(Fqn) except that you can pass in options for this specific method invocation.
     * {@link Option}
     * @param fqn
     * @param option
     * @throws CacheException
     */
    void remove(Fqn fqn, Option option) throws CacheException;

    /**
     * The same as calling remove(Fqn, Object) except that you can pass in options for this specific method invocation.
     * {@link Option}
     * @param fqn
     * @param key
     * @param option
     * @throws CacheException
     */
    Object remove(Fqn fqn, Object key, Option option) throws CacheException;

    /**
     * The same as calling getChildrenNames(Fqn) except that you can pass in options for this specific method invocation.
     * {@link Option}
     * @param fqn
     * @param option
     * @return
     * @throws CacheException
     */
    Set getChildrenNames(Fqn fqn, Option option) throws CacheException;

    /**
     * The same as calling put(Fqn, Map) except that you can pass in options for this specific method invocation.
     * {@link Option}
     * @param fqn
     * @param data
     * @param option
     * @throws CacheException
     */
    void put(Fqn fqn, Map data, Option option) throws CacheException;

    /**
     * The same as calling put(Fqn, Object, Object) except that you can pass in options for this specific method invocation.
     * {@link Option}
     * @param fqn
     * @param key
     * @param value
     * @param option
     * @throws CacheException
     */
    void put(Fqn fqn, Object key, Object value, Option option) throws CacheException;

    // ---------------------------------------------------------------
    // START: Methods to provide backward compatibility with older cache loader config settings
    // ---------------------------------------------------------------

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
     * @deprecated
     */
    void setCacheLoaderClass(String cache_loader_class);

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
     * @deprecated
     */
    void setCacheLoaderConfig(Properties cache_loader_config);

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
     * @deprecated
     */
    void setCacheLoaderShared(boolean shared);

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
     * @deprecated
     */
    void setCacheLoaderPassivation(boolean passivate);

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
     * @deprecated
     */
    void setCacheLoaderPreload(String list);

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
     * @deprecated
     */
    void setCacheLoaderAsynchronous(boolean b);

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
     * @deprecated
     */
    void setCacheLoaderFetchPersistentState(boolean flag);

    /**
     * @see #setCacheLoaderConfiguration(org.w3c.dom.Element) 
     * @deprecated
     */
    void setCacheLoaderFetchTransientState(boolean flag);

    /**
     * Provided for backwards compat.
     *
     * @param loader
     * @deprecated
     */
    void setCacheLoader(CacheLoader loader);

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     *
     * @deprecated
     */
    String getCacheLoaderClass();

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     *
     * @deprecated
     */
    boolean getCacheLoaderShared();

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     *
     * @deprecated
     */
    boolean getCacheLoaderPassivation();

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     *
     * @deprecated
     */
    boolean getCacheLoaderAsynchronous();

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     *
     * @deprecated
     */
    String getCacheLoaderPreload();

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     *
     * @deprecated
     */
    boolean getCacheLoaderFetchPersistentState();

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     *
     * @deprecated
     */
    boolean getCacheLoaderFetchTransientState();

    /**
     * provided for backward compat.  Use getCacheLoaderConfiguration() instead.
     * @deprecated
     */
    Properties getCacheLoaderConfig();

    /**
     * Sets the buddy replication configuration element
     * @param config
     */
    void setBuddyReplicationConfig(Element config);

    /**
     * Retrieves the buddy replication cofiguration element
     * @return config
     */
    Element getBuddyReplicationConfig();

    /**
     * Retrieves the JGroups multiplexer service name if defined.
     * @return the multiplexer service name
     */
    String getMultiplexerService();
    
    /**
     * Sets the JGroups multiplexer service name.
     * This attribute is optional; if not provided, a JGroups JChannel will be used
     * 
     * @param serviceName the multiplexer service name
     */
    void setMultiplexerService(String serviceName);
    
    /**
     * Retrieves the JGroups multiplexer stack name if defined.
     * @return the multiplexer stack name
     */
    String getMultiplexerStack();
    
    /**
     * Used with JGroups multiplexer, specifies stack to be used (e.g., fc-fast-minimalthreads) 
     * This attribute is optional; if not provided, a default multiplexer stack will be used.
     * 
     * @param stackName the name of the multiplexer stack
     */
    void setMultiplexerStack(String stackName);
    
    /**
     * Gets whether this cache using a channel from the JGroups multiplexer.
     * Will not provide a meaningful result until after {@link #startService()}
     * is invoked. 
     */
    boolean isUsingMultiplexer();

    /**
     * Purges the contents of all configured {@link CacheLoader}s
     */
    void purgeCacheLoaders() throws Exception;

   void setUseInterceptorMbeans(boolean b);
}
