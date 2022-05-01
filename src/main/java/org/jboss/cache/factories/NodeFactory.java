/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.factories;

import org.jboss.cache.*;
import org.jboss.cache.optimistic.WorkspaceNodeImpl;
import org.jboss.cache.optimistic.WorkspaceNode;
import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jboss.cache.optimistic.DataVersion;

import java.util.Map;

/**
 * A factory that generates nodes.
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 *
 */
public class NodeFactory
{
   private static NodeFactory _singleton;
   public static final byte NODE_TYPE_TREENODE = 1;
   public static final byte NODE_TYPE_WORKSPACE_NODE = 2;
   public static final byte NODE_TYPE_OPTIMISTIC_NODE = 3;
   /**
    * private empty ctor
    *
    */
   private NodeFactory()
   {
   }
   
   /**
    * Singleton accessor
    * @return a singleton instance of the NodeFactory
    */
   public static NodeFactory getInstance()
   {
      if (_singleton == null) _singleton = new NodeFactory();
      return _singleton;
   }

   /**
    * Returns a new root node.
    */
   public DataNode createRootDataNode(byte nodeType, TreeCache cache)
   {
       return createDataNode(nodeType, null, Fqn.ROOT, (DataNode)null, (Map)null, cache);
   }

   /**
    * Returns a new data node.
    */
   public DataNode createDataNode(byte nodeType, Object childName, Fqn fqn, DataNode parent, Map data, TreeCache cache, DataVersion version)
   {
       return createDataNode(nodeType, childName, fqn, parent, data, false, cache, version);
   }

   /**
    * Returns a new data node.
    */
   public DataNode createDataNode(byte nodeType, Object childName, Fqn fqn, DataNode parent, Map data, TreeCache cache)
   {
      return createDataNode(nodeType, childName, fqn, parent, data, false, cache, null);
   }

   /**
    * Returns a new data node.
    */
    public DataNode createDataNode(byte nodeType, Object childName, Fqn fqn, DataNode parent, Map data, boolean mapSafe, TreeCache cache)
    {
        return createDataNode(nodeType, childName, fqn, parent, data, mapSafe, cache, null);
    }

   /**
    * Creates a new {@link DataNode} instance.
    * 
    * @param nodeType  either {@link #NODE_TYPE_TREENODE} or {@link #NODE_TYPE_TREENODE}
    * @param childName the new node's name
    * @param fqn       the new node's Fqn
    * @param parent    the new node's parent
    * @param data      the new node's attribute map
    * @param mapSafe   <code>true</code> if param <code>data</code> can safely 
    *                  be directly assigned to the new node's data field;
    *                  <code>false</code> if param <code>data</code>'s contents
    *                  should be copied into the new node's data field.
    * @param cache     the cache to which the new node will be added
    * 
    * @return  the new node
    */
   public DataNode createDataNode(byte nodeType, Object childName, Fqn fqn, DataNode parent, Map data, boolean mapSafe, TreeCache cache, DataVersion version)
   {
      DataNode n = null;
      switch (nodeType)
      {
         case NODE_TYPE_TREENODE: n = new Node(childName, fqn, (Node) parent, data, mapSafe, cache); 
            break;
         case NODE_TYPE_OPTIMISTIC_NODE: n = new OptimisticTreeNode(childName, fqn, (Node) parent, data, mapSafe, cache, version);
             break;

      }
      return n;
   }
   
   /**
    * Creates a node of the same type of the node passed in as a template.  The template passed in is entirely unaffected
    * and is not related in any way to the new node except that they will share the same type.
    * @param template
    * @return TreeNode
    */
   public TreeNode createNodeOfType( TreeNode template, Object childName, Fqn fqn, TreeNode parent, Map data, TreeCache cache )
   {
       return createNodeOfType( template, childName, fqn, parent, data, cache, null);
   }

    /**
     * same as above, passing in an explicit version
     */
    public TreeNode createNodeOfType(TreeNode template, Object childName, Fqn fqn, TreeNode parent, Map data, TreeCache cache, DataVersion version)
    {
      // TODO: MANIK - not the most elegant - temporary for now.
      if (template instanceof WorkspaceNode)
      {
          DataNode dataNodeParent = ((WorkspaceNode) parent).getNode();
          TransactionWorkspace workspace = ((WorkspaceNode) template).getTransactionWorkspace();
          return createWorkspaceNode(dataNodeParent, workspace);
      }

      if (parent instanceof DataNode)
      {
          if (template instanceof OptimisticTreeNode) return createDataNode( NODE_TYPE_OPTIMISTIC_NODE, childName, fqn, (DataNode) parent, data, cache, version );
          if (template instanceof Node) return createDataNode( NODE_TYPE_TREENODE, childName, fqn, (DataNode) parent, data, cache );
      }
      return null;
   }

   public WorkspaceNode createWorkspaceNode(TreeNode dataNode, TransactionWorkspace workspace)
   {
       return new WorkspaceNodeImpl( dataNode, workspace );
   }

}
