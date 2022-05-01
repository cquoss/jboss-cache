/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.optimistic;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;

import java.util.Map;
import java.util.TreeMap;
import java.util.SortedMap;

/**
 * Contains a mapping of Fqn to {@link WorkspaceNode}s.
 * Each entry corresponds to a series of changed nodes within the transaction.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Steve Woodcock (<a href="mailto:stevew@jofti.com">stevew@jofti.com</a>)
 */
public class TransactionWorkspaceImpl implements TransactionWorkspace
{

    private static FqnComparator fqnComparator = new FqnComparator();

    private Map nodes;
    private TreeCache cache;
    private boolean versioningImplicit = true;

    public TransactionWorkspaceImpl()
    {
        nodes = new ConcurrentHashMap();
    }

    public void setCache(TreeCache cache)
    {
        this.cache = cache;
    }

    public TreeCache getCache()
    {
        return cache;
    }

    /**
     * Returns the nodes.
     */
    public Map getNodes()
    {
        return nodes;
    }

    /**
     * Sets the nodes.
     */
    public void setNodes(Map nodes)
    {
        this.nodes = nodes;
    }

    public WorkspaceNode getNode(Fqn fqn)
    {
        return (WorkspaceNode) nodes.get(fqn);
    }

    public void addNode(WorkspaceNode node)
    {
        nodes.put(node.getFqn(), node);
    }

    public Object removeNode(Fqn fqn)
    {
        return nodes.remove(fqn);
    }

    public SortedMap getNodesAfter(Fqn fqn)
    {
        SortedMap sm = new TreeMap(fqnComparator);
        sm.putAll( nodes );
        return sm.tailMap(fqn);
    }

    public boolean isVersioningImplicit()
    {
        return versioningImplicit;
    }

    public void setVersioningImplicit(boolean versioningImplicit)
    {
        this.versioningImplicit = versioningImplicit;
    }

    /**
     * Returns debug information.
     */
    public String toString()
    {
        return "Workspace nodes=" + nodes;
    }
}
