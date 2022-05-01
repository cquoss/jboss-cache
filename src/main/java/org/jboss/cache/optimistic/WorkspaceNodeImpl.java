/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.optimistic;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.OptimisticTreeNode;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeNode;
import org.jboss.cache.factories.NodeFactory;
import org.jboss.cache.lock.IdentityLock;
import org.jboss.cache.lock.LockingException;
import org.jboss.cache.lock.TimeoutException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wraps a DataNode and adds versioning and other meta data to it.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Steve Woodcock (<a href="mailto:stevew@jofti.com">stevew@jofti.com</a>)
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 */
public class WorkspaceNodeImpl implements WorkspaceNode
{

    private static Log log = LogFactory.getLog(WorkspaceNodeImpl.class);

    private DataNode node;
    private TransactionWorkspace workspace;
    private DataVersion version = DefaultDataVersion.ZERO;
    private boolean deleted;
    private boolean modified;
    private boolean created;
    private Map optimisticChildNodeMap;
    private Map optimisticDataMap;
   private boolean versioningImplicit = true;
   private boolean childrenModified;
   private Set childrenAdded = new HashSet();
   private Set childrenRemoved = new HashSet();
   private static boolean trace = log.isTraceEnabled();


    public WorkspaceNodeImpl() {
        this(new OptimisticTreeNode(), null);
    }

    /**
     * Constructs with a node and workspace.
     * @deprecated
     */
    public WorkspaceNodeImpl(TreeNode node, TransactionWorkspace workspace)
    {
        this((DataNode)node, workspace);
    }

    /**
     * Constructs with a node and workspace.
     */
    public WorkspaceNodeImpl(DataNode node, TransactionWorkspace workspace)
    {
        if (!(node instanceof OptimisticTreeNode))
           throw new IllegalArgumentException("node " + node + " not OptimisticTreeNode");
        this.node = node;
        this.workspace = workspace;
        optimisticDataMap = node.getData();
        if (optimisticDataMap == null)
            optimisticDataMap = new HashMap();
        if (node.getChildren() == null)
        {
            optimisticChildNodeMap = Collections.EMPTY_MAP;
        }
        else
        {
            optimisticChildNodeMap = new ConcurrentReaderHashMap(node.getChildren());
        }
        this.version = ((OptimisticTreeNode) node).getVersion();
    }


   public boolean isChildrenModified()
   {
      return childrenModified;
   }

   /**
     * Returns true if this node is modified.
     */
    public boolean isModified()
    {
        return modified;
    }

   /**
    * A convenience method that returns whether a node is dirty, i.e., it has been created, deleted or modified.
    * @return true if the node has been either created, deleted or modified.
    */
   public boolean isDirty()
   {
      return modified || created || deleted;
   }

    public Fqn getFqn()
    {
        return node.getFqn();
    }

    public void put(Map data, boolean eraseData)
    {
        realPut(data, eraseData);
        modified = true;
    }

    public void put(Map data)
    {
        realPut(data, false);
        modified = true;
    }

    public Object put(Object key, Object value)
    {
        modified = true;
        return optimisticDataMap.put(key, value);

    }

    public Object remove(Object key)
    {
        modified = true;
        return optimisticDataMap.remove(key);

    }

    public void clear()
    {
        optimisticDataMap.clear();
        modified = true;
    }

    public Object get(Object key)
    {
        return optimisticDataMap.get(key);
    }

    public Set getKeys()
    {
        return optimisticDataMap.keySet();
    }

    //not able to delete from this
    public Set getChildrenNames()
    {
        return new HashSet(optimisticChildNodeMap.keySet());
    }

    private void realPut(Map data, boolean eraseData)
    {
        realPut(data, eraseData, true);
    }

    private void realPut(Map data, boolean eraseData, boolean forceDirtyFlag)
    {
        if (forceDirtyFlag) modified = true;
        if (eraseData)
        {
            optimisticDataMap.clear();
        }
       if (data != null) optimisticDataMap.putAll(data);
    }

    public void removeChild(Object childName)
    {

        childrenModified = true;
        Object child = optimisticChildNodeMap.remove(childName);
       if (trace) log.trace("Removing child " + childName);
       if (child != null)
       {
         childrenRemoved.add(child);
         childrenAdded.remove(child);
       }
    }

    //this needs to be changed to return wrapped node
    public TreeNode getParent()
    {
        return node.getParent();
    }

    //this what the above method should look like
    public TreeNode getWrappedParent()
    {

        //see if in the the transaction map
        WorkspaceNode workspaceNode = workspace.getNode(node.getParent().getFqn());
        if (workspaceNode == null)
        {
            workspaceNode = NodeFactory.getInstance().createWorkspaceNode(node.getParent(), workspace);
            workspace.addNode(workspaceNode);

        }
        return workspaceNode;
    }

    public TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent)
    {
        log.error("Not implemented here!!");
        return null;
    }

    public TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent, TreeCache cache, DataVersion version)
    {
        if (child_name == null)
        {
            return null;
        }

        //see if we already have it
        TreeNode child = (TreeNode) optimisticChildNodeMap.get(child_name);

        // if not we need to create it
        if (child == null)
        {
            child = NodeFactory.getInstance().createNodeOfType(parent, child_name, fqn, parent, null, cache, version);
            if (optimisticChildNodeMap == Collections.EMPTY_MAP)
                optimisticChildNodeMap = new ConcurrentReaderHashMap();
            optimisticChildNodeMap.put(child_name, child);
           if (trace) log.trace("Adding child " + child_name);
           childrenAdded.add(child);
           childrenRemoved.remove(child);
        }

       childrenModified = true;

        if (trace) log.trace("createChild: fqn=" + fqn + " for node " + this);
        return child;

    }

   public boolean isVersioningImplicit()
   {
      return versioningImplicit;
   }

   public void setVersioningImplicit(boolean b)
   {
      versioningImplicit = b;
   }

   //this needs to be changed to return wrapped node
    public TreeNode getChild(Object childName)
    {
        //see if in the the transaction map
        return (TreeNode) optimisticChildNodeMap.get(childName);
    }

    //this what the above method should be like
    public TreeNode getWrappedChild(Object fqn)
    {

        //see if in the the transaction map
        WorkspaceNode wrapper = workspace.getNode((Fqn) fqn);
        if (wrapper == null)
        {
            DataNode temp = (DataNode) optimisticChildNodeMap.get(fqn);
            if (temp != null)
            {
                wrapper = new WorkspaceNodeImpl(temp, workspace);
                workspace.addNode(wrapper);
                // childrenInWorkspace.add( wrapper );
            }

        }
        return wrapper;
    }

    public DataNode getNode()
    {
        return node;
    }

    public DataVersion getVersion()
    {
        return version;
    }

    public void setVersion(DataVersion version)
    {
        this.version = version;
    }

    public List getMergedChildren()
    {
 	    List l = new ArrayList(2);
 	    l.add(childrenAdded);
 	    l.add(childrenRemoved);
 	    return l;
    }

//    private Map mergeMaps(OptimisticMap opMap)
//    {
//        Map temp = new HashMap(opMap.getOriginalMap());
//        //first remove all removed keys
//        for (Iterator it = opMap.getRemovedMap().keySet().iterator(); it.hasNext();)
//        {
//            temp.remove(it.next());
//        }
//        // then add in changed stuff
//        for (Iterator it = opMap.getLocalMap().entrySet().iterator(); it.hasNext();)
//        {
//            Map.Entry entry = (Map.Entry) it.next();
//            temp.put(entry.getKey(), entry.getValue());
//        }
//        return temp;
//        TODO: MANIK: BN: Does this need to be a copy?!??
//        return new HashMap(opMap.getLocalMap());
//        return opMap.getLocalMap();
//    }

    public Map getMergedData()
    {
        return optimisticDataMap;
    }

    public void markAsDeleted(boolean marker)
    {
       markAsDeleted(marker, false);
    }

    public void markAsDeleted(boolean marker, boolean recursive)
    {
       deleted = marker;
       if (recursive && optimisticChildNodeMap != null)
       {
          synchronized (this)
          {
             Collection values = optimisticChildNodeMap.values();
             for (Iterator it=values.iterator(); it.hasNext();)
             {
                ((WorkspaceNodeImpl) it.next()).markAsDeleted(marker, true);
             }
          }
       }
    }

    public boolean isDeleted()
    {
        return deleted;
    }

    public Object getName()
    {
        return node.getName();
    }

    public TransactionWorkspace getTransactionWorkspace()
    {
        return workspace;
    }

    public boolean isCreated()
    {
        return created;
    }

    public void markAsCreated()
    {
        created = true;
       // created != modified!!
//        modified = true;
    }

    /**
     * Always returns null; dummy method for TreeNode compatibility.
     * @return null
     */
    public Map getData()
    {
        return null;
    }

    /**
     * Always returns null; dummy method for TreeNode compatibility.
     * @return null
     */
    public Map getChildren()
    {
        return null;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public boolean containsKey(Object key)
    {
        return false;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public Set getDataKeys()
    {
        return null;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public boolean childExists(Object child_name)
    {
        return false;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public IdentityLock getImmutableLock()
    {
        return null;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public IdentityLock getLock()
    {
        return null;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public int numAttributes()
    {
        return 0;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public boolean hasChildren()
    {
        return false;
    }

    /**
     * Creates a new child of this node if it doesn't exist. Also notifies the cache
     * that the new child has been created.
     * dded this new getOrCreateChild() method to avoid thread contention
     * on create_lock in PessimisticLockInterceptor.lock()
     *
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public TreeNode getOrCreateChild(Object child_name, GlobalTransaction gtx, boolean createIfNotExists)
    {
        return null;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent, Object key, Object value)
    {
        return null;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void removeAllChildren()
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void print(StringBuffer sb, int indent)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void printDetails(StringBuffer sb, int indent)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void printIndent(StringBuffer sb, int indent)
    {
    }

    /**
     * Adds the (already created) child node. Replaces existing node if present.
     *
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void addChild(Object child_name, TreeNode n)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void printLockInfo(StringBuffer sb, int indent)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public boolean isLocked()
    {
        return false;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void releaseAll(Object owner)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void releaseAllForce()
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public Set acquireAll(Object caller, long timeout, int lock_type) throws LockingException, TimeoutException, InterruptedException
    {
        return null;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void setRecursiveTreeCacheInstance(TreeCache cache)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public boolean getChildrenLoaded()
    {
        return false;
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void setChildrenLoaded(boolean b)
    {
    }

    /**
     * Sets Map<Object,TreeNode>
     *
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void setChildren(Map children)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void release(Object caller)
    {
    }

    /**
     * @see org.jboss.cache.DataNode
     * @deprecated Will be removed in JBossCache 1.3.
     */
    public void releaseForce()
    {
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        if (deleted) sb.append("del ");
        if (modified) sb.append("modified ");
        if (created) sb.append("new ");
        return
          "WorkNode fqn=" + getFqn() + " " + sb + "ver=" + version;
    }

    public void addChild(WorkspaceNode child)
    {
       if (trace) log.trace("Adding child " + child.getName());
       optimisticChildNodeMap.put(child.getName(), child.getNode());
       childrenAdded.add(child.getNode());
       childrenRemoved.remove(child.getNode());
       childrenModified = true;
    }
}
