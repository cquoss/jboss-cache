/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;

import org.jboss.cache.lock.IdentityLock;
import org.jboss.cache.lock.LockingException;
import org.jboss.cache.lock.TimeoutException;

import java.util.Map;
import java.util.Set;

/**
 * Represents a node in the tree. Has a relative name and a Fqn. Maintains a
 * hashmap of general data.  If the node is created in a replicated cache, the
 * relative and fully qualified name, and the keys and values of the hashmap
 * must be serializable.
 * <p>
 * The current version supports different levels of transaction locking such as
 * simple locking ({@link org.jboss.cache.lock.IsolationLevel#SERIALIZABLE}, or Read/Write lock with
 * upgrade ( {@link org.jboss.cache.lock.IsolationLevel#REPEATABLE_READ}) --that is the read lock will be
 * automatically upgraded to write lock when the same owner intends to modify
 * the data after read.
 * <p>
 * Implementations may not necessarily be <em>not</em> synchronized, so access
 * to instances of TreeNode need to be run under an isolation level above NONE.
 *
 * @author Bela Ban March 25 2003
 * @author Ben Wang
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @version $Revision: 2058 $
 */

public interface TreeNode
{

    // TODO: MANIK: are all these params necessary? - to be investigated
    /**
     * Creates a child node with a name, FQN, and parent.
     * Returns the created node.
     */
    TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent);

    /**
     * Returns the fully qualified name of the node.
     */
    Fqn getFqn();

    /**
     * Returns the named child of this node.
     */
    TreeNode getChild(Object childName);

    /**
     * Removes the named child of this node.
     */
    void removeChild(Object childName);

    /**
     * Returns the parent of this node.
     */
    TreeNode getParent();

    /**
     * Puts the contents of a map into this node.
     * Optionally erases the existing contents.
     * @param eraseData true to erase the existing contents
     */
    void put(Map data, boolean eraseData);

    /**
     * Puts the key and value into the node.
     * Returns the old value of the key, if it existed.
     */
    Object put(Object key, Object value);

    /**
     * Removes the old value of the key.
     */
    Object remove(Object key);

    /**
     * Returns the value of a key or null if it does not exist.
     */
    Object get(Object key);

    /**
     * Clears the data of this node.
     */
    void clear();

    /**
     * Adds (merges) the contents of the map with the existing data.
     */
    void put(Map data);

    /**
     * Returns the name of this node.
     */
    Object getName();

    /**
     * Prints the node and children.
     */ 
    void print(StringBuffer sb, int indent);

    /**
     * Prints the node with details and indent.
     */ 
    void printDetails(StringBuffer sb, int indent);

    /**
     * Prints the node with indent.
     */ 
    void printIndent(StringBuffer sb, int indent);

    /**
     * Returns true if the key is in the data set.
     */
    boolean containsKey(Object key);

    /**
     * Returns an unmodifiable map, mapping keys to child nodes.
     * Implementations need to make sure the map cannot be changed.
     *
     * @return Map<Object,TreeNode>
     */
    Map getChildren();

    /**
     * Returns the data keys, or an empty set if there are no keys.
     */
    Set getDataKeys();

    /**
     * Returns true if the child exists.
     */
    boolean childExists(Object child_name);

    /**
     * Returns the number of attributes.
     */
    int numAttributes();

    /**
     * Returns true if this node has children.
     */
    boolean hasChildren();

    /**
     * Creates a child node.
     */
    TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent, Object key, Object value);

    /**
     * Removes all children.
     */
    void removeAllChildren();

    /**
     * Adds the already created child node.
     * Replaces the existing child node if present.
     */
    void addChild(Object child_name, TreeNode n);

    // ---- deprecated methods - should use similar meths in DataNode or AbstractNode instead ---
    // ---- these deprecated methods will be removed in JBossCache 1.3. ---

    /**
     * Returns a copy of the attributes.  Use get(Object key) instead.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    Map getData();

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    IdentityLock getImmutableLock();

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    IdentityLock getLock();

    /**
     * Creates a new child of this node if it doesn't exist. Also notifies the cache
     * that the new child has been created.
     * dded this new getOrCreateChild() method to avoid thread contention
     * on create_lock in PessimisticLockInterceptor.lock()
     *
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     */
    TreeNode getOrCreateChild(Object child_name, GlobalTransaction gtx, boolean createIfNotExists);

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void printLockInfo(StringBuffer sb, int indent);

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    boolean isLocked();

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void releaseAll(Object owner);

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void releaseAllForce();

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    Set acquireAll(Object caller, long timeout, int lock_type)
            throws LockingException, TimeoutException, InterruptedException;

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void setRecursiveTreeCacheInstance(TreeCache cache);

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    boolean getChildrenLoaded();

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void setChildrenLoaded(boolean b);

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * Sets Map<Object,TreeNode>
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void setChildren(Map children);

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void release(Object caller);

    /**
     * Not to be exposed.  Internal calls should use impl classes.
     * @see DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    void releaseForce();

}
