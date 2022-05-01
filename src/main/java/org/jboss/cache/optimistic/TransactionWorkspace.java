/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.optimistic;

import org.jboss.cache.Fqn;

import java.util.Map;
import java.util.SortedMap;

/**
 * Used to contain a copy of the tree being worked on within the scope of a given transaction.  Maintains {@see WorkspaceNode}s rather than conventional
 * {@see DataNode}s.  Also see {@see OptimisticTransactionEntry}, which creates and maintains an instance of TransactionWorkspace for each
 * transaction running.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Steve Woodcock (<a href="mailto:stevew@jofti.com">stevew@jofti.com</a>)
 */

public interface TransactionWorkspace
{
    /**
     * @return Returns a map of {@link WorkspaceNode}s, keyed on {@link Fqn}
     */
    public Map getNodes();

    /**
     * @param nodes The nodes to set. Takes {@link WorkspaceNode}s.
     */
    public void setNodes(Map nodes);

    public WorkspaceNode getNode(Fqn fqn);

    /**
     *  Is thread safe so you dont need to deal with synchronising access to this method.
     *
     * @param node
     */
    public void addNode(WorkspaceNode node);

    /**
     *  Is thread safe so you dont need to deal with synchronising access to this method.
     *
     */
    public Object removeNode(Fqn fqn);

    /**
     * Returns all nodes equal to or after the given node.
     */
    public SortedMap getNodesAfter(Fqn fqn);

    /**
     * Tests if versioning is implicit for a given tx.
     * If set to true, the interceptor chain will handle versioning (implicit to JBossCache).
     * If set to false, DataVersions will have to come from the caller.
     */
    public boolean isVersioningImplicit();

    /**
     * Sets if versioning is implicit for a given tx.
     * If set to true, the interceptor chain will handle versioning (implicit to JBossCache).
     * If set to false, DataVersions will have to come from the caller.
     */
    public void setVersioningImplicit(boolean versioningImplicit);
}
