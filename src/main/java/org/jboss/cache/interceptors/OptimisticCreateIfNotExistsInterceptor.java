/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.interceptors;

import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.OptimisticTransactionEntry;
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TransactionTable;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeNode;
import org.jboss.cache.factories.NodeFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.optimistic.DataVersion;
import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jboss.cache.optimistic.WorkspaceNode;
import org.jgroups.blocks.MethodCall;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
* Used to make copies of nodes from the main tree into the {@see TransactionWorkspace} as and when needed.
*
* @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
* @author <a href="mailto:stevew@jofti.com">Steve Woodcock (stevew@jofti.com)</a>
*/
public class OptimisticCreateIfNotExistsInterceptor extends OptimisticInterceptor
{

    public void setCache(TreeCache cache)
    {
        super.setCache(cache);
    }

    public Object invoke(MethodCall m) throws Throwable
    {
       //should this be just put methods
        if (MethodDeclarations.isOptimisticPutMethod(m.getMethod()))
        {
            Object[] args = m.getArgs();
            Fqn fqn = (Fqn) (args != null ? args[1] : null);
            if (fqn == null)
            {
                throw new CacheException("failed extracting FQN from method " + m);
            }
            if (!cache.exists(fqn))
            {
                GlobalTransaction gtx = getInvocationContext().getGlobalTransaction();
                if (gtx != null)
                {
                    createNode(fqn, gtx, txTable);
                }
                else
                {
                    throw new CacheException("no transaction or temporary transaction found " + m);
                }

            }
        }
        return super.invoke(m);
    }

    /**
     * The only method that should be creating nodes.
     *
     * @param fqn
     * @param gtx
     * @param tx_table
     * @throws CacheException
     */
    private void createNode(Fqn fqn, GlobalTransaction gtx, TransactionTable tx_table)
            throws CacheException
    {

        // we do nothing if fqn is null
        if (fqn == null)
        {
            return;
        }

        // get the transaction to create the nodes in
        TransactionEntry baseTransactionEntry = tx_table.get(gtx);
        OptimisticTransactionEntry transactionEntry = (OptimisticTransactionEntry) baseTransactionEntry;

        if (transactionEntry == null)
        {
            throw new CacheException("Unable to map global transaction " + gtx + " to transaction entry");
        }

        WorkspaceNode workspaceNode, childWorkspaceNode = null;
        Object childName;

        List nodesCreated = new ArrayList();
        // how many levels do we have?
        int treeNodeSize = fqn.size();

        InvocationContext ctx = getInvocationContext();

        // try and get the root from the transaction
        TransactionWorkspace workspace = transactionEntry.getTransactionWorkSpace();

        synchronized( workspace )
        {
            DataVersion version = null;
            if (ctx.getOptionOverrides() != null && ctx.getOptionOverrides().getDataVersion() != null)
            {
                version = ctx.getOptionOverrides().getDataVersion();
                workspace.setVersioningImplicit( false );
            }

            if (log.isDebugEnabled()) log.debug(" Getting root fqn from workspace  for gtx " + gtx);
            workspaceNode = workspace.getNode(cache.getRoot().getFqn());

            // we do not have the root so lets wrap it in case we need to add it
            // to the transaction
            if (workspaceNode == null)
            {
                workspaceNode = NodeFactory.getInstance().createWorkspaceNode(cache.getRoot(), workspace);
                workspace.addNode(workspaceNode);
                if (log.isDebugEnabled()) log.debug(" created root node " + workspaceNode + " in workspace " + gtx);
            }
            else
            {
                if (log.isDebugEnabled()) log.debug(" Already found root node " + workspaceNode + " in workspace " + gtx);
            }

            // we will always have one root node here, by this stage
            Fqn tmpFqn = Fqn.ROOT;
            Fqn copy;
            for (int i = 0; i < treeNodeSize; i++)
            {
                boolean isTargetFqn = (i == (treeNodeSize - 1));
                childName = fqn.get(i);

                // build up intermediate node fqn from original Fqn
                tmpFqn = new Fqn(tmpFqn, childName);

                // current workspace node canot be null.
                // try and get the child of current node

                if (log.isTraceEnabled()) log.trace(" Entering synchronized nodewrapper access  for gtx " + gtx);
                TreeNode tempChildNode = workspaceNode.getChild(childName);

//                if (log.isDebugEnabled()) log.debug(" Entered synchronized workspaceNode " + workspaceNode + " access  for gtx " + gtx);

                // no child exists with this name
                if (tempChildNode == null)
                {
                    if (log.isTraceEnabled()) log.trace("Child node "+childName+" doesn't exist.  Creating new node.");
                    // we put the parent node into the workspace as we are changing it's children
                    WorkspaceNode tempCheckWrapper = workspace.getNode(workspaceNode.getFqn());
                    if (tempCheckWrapper == null || tempCheckWrapper.isDeleted())
                    {
                        //add a new one or overwrite an existing one that has been deleted
                        if (log.isTraceEnabled()) log.trace("Parent node "+workspaceNode.getFqn()+" doesn't exist in workspace or has been deleted.  Adding to workspace in gtx " + gtx);
                        workspace.addNode(workspaceNode);
                    }
                    else
                    {
                        if (log.isTraceEnabled()) log.trace(" Parent node " + workspaceNode.getFqn() + " exists in workspace " + gtx);
                    }
                    copy = (Fqn) tmpFqn.clone();
                    // this does not add it into the real child nodes - but in its
                    // local child map for the transaction

                    // get the version passed in, if we need to use explicit versioning.
                    DataVersion versionToPassIn = null;
                    if (isTargetFqn && !workspace.isVersioningImplicit()) versionToPassIn = version;

                    DataNode tempNode = (DataNode) workspaceNode.createChild(childName, copy, workspaceNode.getNode(), cache, versionToPassIn);

                    childWorkspaceNode = NodeFactory.getInstance().createWorkspaceNode(tempNode, workspace);
                   childWorkspaceNode.setVersioningImplicit(versionToPassIn == null || !isTargetFqn);
                   if (log.isTraceEnabled()) log.trace("setting versioning of " + childWorkspaceNode.getFqn() + " to be " + (childWorkspaceNode.isVersioningImplicit() ? "implicit" : "explicit"));

                    // now add the wrapped child node into the transaction space
                    workspace.addNode(childWorkspaceNode);
                    childWorkspaceNode.markAsCreated();
                    // save in list so we can broadcast our created nodes outside
                    // the synch block
                    nodesCreated.add(tmpFqn);

                }
                else
                {
                    // node does exist but might not be in the workspace
                    childWorkspaceNode = workspace.getNode(tempChildNode.getFqn());
                    // wrap it up so we can put it in later if we need to
                    if (childWorkspaceNode == null || childWorkspaceNode.isDeleted())
                    {
                        if (log.isDebugEnabled()) log.debug("Child node "+tempChildNode.getFqn()+" doesn't exist in workspace or has been deleted.  Adding to workspace in gtx " + gtx);
                        childWorkspaceNode = NodeFactory.getInstance().createWorkspaceNode(tempChildNode, workspace);
                        if (isTargetFqn && !workspace.isVersioningImplicit())
                        {
                           childWorkspaceNode.setVersion(version);
                           childWorkspaceNode.setVersioningImplicit(false);
                        }
                       else
                        {
                           childWorkspaceNode.setVersioningImplicit(true);
                        }
                       if (log.isTraceEnabled()) log.trace("setting versioning of " + childWorkspaceNode.getFqn() + " to be " + (childWorkspaceNode.isVersioningImplicit() ? "implicit" : "explicit"));
                       
                    }
                    else
                    {
                        if (log.isDebugEnabled()) log.debug(" Already found " + tempChildNode.getFqn() + " node in workspace " + gtx);

                    }
                }
                workspaceNode = childWorkspaceNode;
            }

            if (log.isTraceEnabled()) log.trace("left synchronized nodewrapper access  for gtx " + gtx);
        } // end sync block
        // run the notify outside the synch block as we do not know what that
        // code might do
        if (nodesCreated.size() > 0)
        {
            for (Iterator it = nodesCreated.iterator(); it.hasNext();)
            {
                Object temp = it.next();
                cache.notifyNodeCreated((Fqn) temp);
                if (log.isDebugEnabled()) log.debug("Notifying cache of node created in workspace " + temp);
            }
        }
    }

}
