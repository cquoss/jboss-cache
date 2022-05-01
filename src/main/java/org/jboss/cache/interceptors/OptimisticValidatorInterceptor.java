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
import org.jboss.cache.OptimisticTreeNode;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeNode;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.optimistic.DataVersioningException;
import org.jboss.cache.optimistic.DefaultDataVersion;
import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jboss.cache.optimistic.WorkspaceNode;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates the data in the transaction workspace against data in the actual
 * cache (versions only), and then performs commits if necessary.  Does not
 * pass on prepare/commit/rollbacks to the other interceptors.
 * <p/>
 * Currently uses simplistic integer based versioning and validation. Plans are
 * to have this configurable as there will always be a performance/complexity
 * tradeoff.
 * <p/>
 * On the commit it applies the changes in the workspace to the real nodes in
 * the cache.
 * <p/>
 * On rollback clears the nodes in the workspace.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Steve Woodcock (<a href="mailto:stevew@jofti.com">stevew@jofti.com</a>)
 */
public class OptimisticValidatorInterceptor extends OptimisticInterceptor
{

    public void setCache(TreeCache cache)
    {
        super.setCache(cache);
    }

    public Object invoke(MethodCall call) throws Throwable
    {
        JBCMethodCall m = (JBCMethodCall) call;
        InvocationContext ctx = getInvocationContext();
        Transaction tx = ctx.getTransaction();
        GlobalTransaction gtx = ctx.getGlobalTransaction();
        Object retval = null;
       
       // bypass for buddy group org metod calls.
       if (isBuddyGroupOrganisationMethod(m)) return super.invoke(m);


        if (tx == null)
            throw new CacheException("Not in a transaction");

        // Methods we are interested in are prepare/commit
        // They do not go further than this interceptor
        switch (m.getMethodId()) 
        {
           case MethodDeclarations.optimisticPrepareMethod_id:
              // should pass in a different prepare here
              validateNodes(gtx);
              break;
           case MethodDeclarations.commitMethod_id:
              commit(gtx);
              break;
           case MethodDeclarations.rollbackMethod_id:
              rollBack(gtx);
              break;
           default :
              retval = super.invoke(m);
              break;
        }
        return retval;
    }


    private void validateNodes(GlobalTransaction gtx) throws CacheException
    {
        TransactionWorkspace workspace;

        try
        {
            workspace = getTransactionWorkspace(gtx);
        }
        catch (CacheException e)
        {
            throw new CacheException("unable to retrieve workspace", e);
        }

        // should be an ordered list - get the set of nodes
        Collection nodes = workspace.getNodes().values();

        boolean validated;
        //we have all locks here so lets try and validate
        log.debug("validating nodes");
        simpleValidate(nodes);
        log.debug("validated nodes");
    }

    private void simpleValidate(Collection nodes)
        throws CacheException
    {
        WorkspaceNode workspaceNode;

        boolean trace = log.isTraceEnabled();
        for (Iterator it = nodes.iterator(); it.hasNext();)
        {
           workspaceNode = (WorkspaceNode) it.next();
           if (workspaceNode.isDirty())
           {
               Fqn fqn = workspaceNode.getFqn();
               if (trace) log.trace("Validating version for node [" + fqn + "]");
               OptimisticTreeNode realNode = (OptimisticTreeNode) cache._get(fqn);

               // if this is a newly created node then we expect the underlying node to be null.
               // also, if the node has been deleted in the WS and the underlying node is null, this *may* be ok ... will test again later when comparing versions
               // if not, we have a problem...
               if (realNode == null && !workspaceNode.isCreated() && !workspaceNode.isDeleted())
               {
                   throw new DataVersioningException("Real node for " + fqn + " is null, and this wasn't newly created in this tx!");
               }

              // needs to have been created AND modified - we allow concurrent creation if no data is put into the node
              if (realNode != null && workspaceNode.isCreated() && workspaceNode.isModified())
              {
                 throw new DataVersioningException("Tx attempted to create " + fqn + " anew.  It has already been created since this tx started by another (possibly remote) tx.");
              }

               if (!workspaceNode.isCreated() && (workspaceNode.isDeleted() || workspaceNode.isModified()))
               {
                  // if the real node is null, throw a DVE
                  if (realNode == null)
                  {
                     // but not if the WSN has also been deleted
                     if (!workspaceNode.isDeleted())
                        throw new DataVersioningException("Unable to compare versions since the underlying node has been deleted by a concurrent transaction!");
                     else
                        if (trace) log.trace("The data node ["+fqn+"] is null, but this is ok since the workspace node is marked as deleted as well");
                  }
                  else if (realNode.getVersion().newerThan( workspaceNode.getVersion() ))
                  {
                     // newerThan() will internally test if the DataVersion types being compared tally up, and will barf if
                     // necessary with an appropriate DataVersioningException.
                     // we have an out of date node here
                     throw new DataVersioningException("DataNode [" + fqn + "] version " + ((OptimisticTreeNode)workspaceNode.getNode()).getVersion() + " is newer than workspace node " + workspaceNode.getVersion());
                  }
               }
           }
           else
           {
               if (trace) log.trace("Node [" + workspaceNode.getFqn() + "] doesn't need validating as it isn't dirty");
           }
        }
    }

    private void commit(GlobalTransaction gtx)
    {
        TransactionWorkspace workspace;

        try
        {
            workspace = getTransactionWorkspace(gtx);
        }
        catch (CacheException e)
        {
            log.trace("we can't rollback", e);
            return;
        }

        log.debug("commiting validated changes ");
        // should be an ordered list
        Collection nodes = workspace.getNodes().values();

        boolean trace = log.isTraceEnabled();
        for (Iterator it = nodes.iterator(); it.hasNext();)
        {
            WorkspaceNode wrappedNode = (WorkspaceNode) it.next();
           if (trace) log.trace("Analysing node " + wrappedNode.getFqn() + " in workspace.");
            // short circuit if this node is deleted?
            if (wrappedNode.isDeleted())
            {
                // handle notifications

                if (trace) log.trace("Node's been deleted; removing");
                DataNode dNode = wrappedNode.getNode();
                cache.notifyNodeRemove(dNode.getFqn(), true);

                if (dNode.getFqn().isRoot())
                {
                    log.warn("Attempted to delete the root node");
                }
                else
                {
                    DataNode parent = (DataNode) dNode.getParent();
                    parent.removeChild( dNode.getName() );
                }
                cache.notifyNodeRemoved(dNode.getFqn());
                cache.notifyNodeRemove(dNode.getFqn(), false);
            }
            else
            {
                // "Will somebody please think of the children!!"
                // if (wrappedNode.hasCreatedOrRemovedChildren() handleChildNodes(wrappedNode);
                //if (wrappedNode.isModified())
                //{
              OptimisticTreeNode current = (OptimisticTreeNode) wrappedNode.getNode();
              boolean updateVersion = false;

              if (wrappedNode.isChildrenModified())
              {
                 log.trace("Updating children since node has modified children");
 	              // merge children.
                 List deltas = wrappedNode.getMergedChildren();

                 if (trace) log.trace("Applying children deltas to parent node " + current.getFqn());
 	              for (Iterator i = ((Set) deltas.get(0)).iterator(); i.hasNext();)
 	              {
                     TreeNode child = (TreeNode) i.next();
                     current.addChild(child.getName(), child);
 	              }

 	              for (Iterator i = ((Set) deltas.get(1)).iterator(); i.hasNext();)
                 {
                    TreeNode child = (TreeNode) i.next();
                    current.removeChild(child.getName());
                 }

                  updateVersion = cache.getLockParentForChildInsertRemove();
              }

               if (wrappedNode.isModified() || wrappedNode.isCreated())
               {
                    cache.notifyNodeModify(wrappedNode.getFqn(), true);

                    log.trace("Merging data since node is dirty");
                    Map mergedData = wrappedNode.getMergedData();
                    current.put(mergedData, true);
                    updateVersion = true;
                    cache.notifyNodeModified(wrappedNode.getFqn());
                    cache.notifyNodeModify(wrappedNode.getFqn(), false);
               }
               
               if (updateVersion)
               {
                    if (wrappedNode.isVersioningImplicit())
                    {
                        if (trace) log.trace("Versioning is implicit; incrementing.");
                        if (wrappedNode.getVersion() instanceof DefaultDataVersion)
                           current.setVersion(((DefaultDataVersion)wrappedNode.getVersion()).increment());
                       else
                           log.debug("Even though no explicit version was passed in, node has an external DataVersion impl.  Don't know how to increment, not incrementing.");
                    }
                    else
                    {
                        if (trace) log.trace("Versioning is explicit; not attempting an increment.");
                        current.setVersion(wrappedNode.getVersion());
                    }
                    if (trace) log.trace("Setting version of node from " + wrappedNode.getVersion() + " to " + current.getVersion());
                }
                else
                {
                    if (trace) log.trace("Version update on " + wrappedNode.getFqn() + " not necessary since the node is not dirty or LockParentForChildInsertRemove is set to false");
                    cache.notifyNodeVisited(wrappedNode.getFqn());
                }
            }
        }

    }

    private void rollBack(GlobalTransaction gtx)
    {
        TransactionWorkspace workspace;
        try
        {
            workspace = getTransactionWorkspace(gtx);
            Map nodes = workspace.getNodes();
            nodes.clear();
        }
        catch (CacheException e)
        {
            log.info("Unable to roll back", e);
        }
    }
}
