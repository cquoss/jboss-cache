/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.optimistic;

import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a type of {@link org.jboss.cache.Node} that is to be copied into a {@link TransactionWorkspace} for optimistically locked
 * nodes.  Adds versioning and dirty flags over conventional Nodes.
 * 
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Steve Woodcock (<a href="mailto:stevew@jofti.com">stevew@jofti.com</a>)
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 */
public interface WorkspaceNode extends TreeNode
{
   /**
    * @return Returns 2 Sets - a set of children added (first set) and a set of children removed.
    */
    List getMergedChildren();

    DataVersion getVersion();

    void setVersion(DataVersion version);

    Set getKeys();

   /**
    * A node is considered modified if it's data map has changed.  If children are added or removed, the node is not
    * considered modified - instead, see {@link #isChildrenModified()}.
    *
    * @return true if the data map has been modified
    */
   boolean isModified();

   /**
    * A convenience method that returns whether a node is dirty, i.e., it has been created, deleted or modified.
    * Noe that adding or removing children does not constitute a node being dirty - see {@link #isChildrenModified()}
    *
    * @return true if the node has been either created, deleted or modified.
    */
   boolean isDirty();

    Map getMergedData();

    DataNode getNode();

    Set getChildrenNames();

    boolean isDeleted();

    void markAsDeleted(boolean marker);

    TransactionWorkspace getTransactionWorkspace();

    boolean isCreated();

    void markAsCreated();

    TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent, TreeCache cache, DataVersion version);

    boolean isVersioningImplicit();

    void setVersioningImplicit(boolean b);

    void addChild(WorkspaceNode workspaceNode);

   /**
 	 * @return true if children have been added to or removed from this node.  Not the same as 'dirty'.
 	 */
 	boolean isChildrenModified();
}
