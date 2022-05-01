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
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeNode;
import org.jboss.cache.config.Option;
import org.jboss.cache.factories.NodeFactory;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.optimistic.DataVersion;
import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jboss.cache.optimistic.WorkspaceNode;
import org.jboss.cache.optimistic.DefaultDataVersion;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Transaction;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Operations on nodes are done on the copies that exist in the workspace rather than passed down to the {@see CallInterceptor}
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:stevew@jofti.com">Steve Woodcock (stevew@jofti.com)</a>
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 */
public class OptimisticNodeInterceptor extends OptimisticInterceptor
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
      Method meth = m.getMethod();
      Object[] args = m.getArgs();

      // bypass for buddy group org metod calls.
      if (isBuddyGroupOrganisationMethod(m)) return super.invoke(m);


      Object result = null;

      GlobalTransaction gtx = ctx.getGlobalTransaction();

      TransactionWorkspace workspace = getTransactionWorkspace(gtx);

      if (MethodDeclarations.isCrudMethod(meth))
      {
         if (tx == null || !isValid(tx))
         {
            throw new CacheException("Must be in a valid transaction " + m);
         }

         WorkspaceNode workspaceNode = getOrCreateWorkspaceNode(getFqn(args), workspace, true);
         if (workspaceNode == null && m.getMethodId() == MethodDeclarations.dataGravitationCleanupMethod_id)
         {
            workspaceNode = getOrCreateWorkspaceNode(getBackupFqn(args), workspace, true);
         }


         if (workspaceNode != null)
         {
            // use explicit versioning
            if (ctx.getOptionOverrides() != null && ctx.getOptionOverrides().getDataVersion() != null)
            {
               workspace.setVersioningImplicit(false);
               DataVersion version = ctx.getOptionOverrides().getDataVersion();

               workspaceNode.setVersion(version);
               if (log.isTraceEnabled()) log.trace("Setting versioning for node " + workspaceNode.getFqn() + " to explicit");
               workspaceNode.setVersioningImplicit(false);
            }
            else
            {
               if (log.isTraceEnabled()) log.trace("Setting versioning for node " + workspaceNode.getFqn() + " to implicit");
               workspaceNode.setVersioningImplicit(true);
            }
         }
         else
         {
            // "fail-more-silently" patch thanks to Owen Taylor - JBCACHE-767
            if ((ctx.getOptionOverrides() == null || !ctx.getOptionOverrides().isFailSilently()) && MethodDeclarations.isOptimisticPutMethod(meth))
               throw new CacheException("Unable to set node version for " + getFqn(args) + ", node is null.");
            else
            {
               log.trace("Workspace node is null.  Perhaps it has been deleted?");
               return null;
            }
         }

         switch (m.getMethodId())
         {
            case MethodDeclarations.putDataMethodLocal_id:
               Boolean erase = (Boolean) args[3];
               putDataMap(args, erase.booleanValue(), workspace, workspaceNode);
               break;
            case MethodDeclarations.putDataEraseMethodLocal_id:
               putDataMap(args, true, workspace, workspaceNode);
               break;
            case MethodDeclarations.putKeyValMethodLocal_id:
               result = putDataKeyValue(args, workspace, workspaceNode);
               break;
            case MethodDeclarations.removeNodeMethodLocal_id:
               removeNode(workspace, workspaceNode);
               break;
            case MethodDeclarations.removeKeyMethodLocal_id:
               result = removeKey(args, workspace, workspaceNode);
               break;
            case MethodDeclarations.removeDataMethodLocal_id:
               removeData(workspace, workspaceNode);
               break;
            case MethodDeclarations.dataGravitationCleanupMethod_id:
               result = super.invoke(m);
            default:
               if (log.isInfoEnabled()) log.info("Cannot Handle Method " + m);
               break;
         }

         Option opt = ctx.getOptionOverrides();
         if (opt == null || !opt.isCacheModeLocal())
         {
            txTable.addModification(gtx, m);
            if (log.isDebugEnabled()) log.debug("Adding Method " + m + " to modification list");
         }
         if (cache.getCacheLoaderManager() != null) txTable.addCacheLoaderModification(gtx, m);
      }
      else
      {
         switch (m.getMethodId())
         {
            case MethodDeclarations.getKeyValueMethodLocal_id:
               result = getValueForKey(args, workspace);
               break;
            case MethodDeclarations.getKeysMethodLocal_id:
               result = getKeys(args, workspace);
               break;
            case MethodDeclarations.getChildrenNamesMethodLocal_id:
               result = getChildNames(args, workspace);
               break;
            case MethodDeclarations.getNodeMethodLocal_id:
               result = getNode(args, workspace);
               break;
            case MethodDeclarations.evictNodeMethodLocal_id:
            case MethodDeclarations.evictVersionedNodeMethodLocal_id:
               result = super.invoke(m);
               break;
            default:
               if (log.isInfoEnabled())
                  log.info("read Method " + m + " called - don't know how to handle, passing on!");
               result = super.invoke(m);
               break;
         }
      }
      return result;
   }

   private Fqn getFqn(Object[] args)
   {
      return (Fqn) args[1];
   }

   private Fqn getBackupFqn(Object[] args)
   {
      return (Fqn) args[2];
   }

   private void putDataMap(Object[] args, boolean eraseExisitng,
                           TransactionWorkspace workspace, WorkspaceNode workspaceNode)
   {

      Map data = (Map) args[2];
      if (workspaceNode == null)
         return;
      workspaceNode.put(data, eraseExisitng);
      workspace.addNode(workspaceNode);
   }

   private Object putDataKeyValue(Object[] args, TransactionWorkspace workspace, WorkspaceNode workspaceNode)
   {

      Object key = args[2];
      Object value = args[3];

      if (workspaceNode == null)
      {
         return null;// this should be an exception
      }

      Object old = workspaceNode.put(key, value);
      workspace.addNode(workspaceNode);
      return old;
   }

   private void removeNode(TransactionWorkspace workspace, WorkspaceNode workspaceNode) throws CacheException
   {
      if (log.isTraceEnabled())
         log.trace("removeNode " + workspace + " node=" + workspaceNode);

      // it is already removed - we can ignore it
      if (workspaceNode == null)
         return;

      // get the parent
      TreeNode temp = workspaceNode.getParent();

      // can parent be null?
      if (temp == null)
         return;

      boolean debug = log.isDebugEnabled();

      Fqn parentFqn = temp.getFqn();

      // get a wrapped parent
      WorkspaceNode parentNode = getOrCreateWorkspaceNode(parentFqn, workspace, false);
      if (parentNode == null)
      {
         // chk if this has been removed in the same tx
         parentNode = workspace.getNode(parentFqn);
         if (parentNode == null || !parentNode.isDeleted())
            throw new CacheException("Unable to find parent node with Fqn " + parentFqn);
      }

      parentNode.removeChild(workspaceNode.getName());
      workspace.addNode(parentNode);
      if (debug) log.debug("added parent node " + parentNode.getFqn() + " to workspace");
      Fqn nodeFqn = workspaceNode.getFqn();

      // Mark this node and all children as deleted
      workspace.addNode(workspaceNode); // deleted below
      SortedMap tailMap = workspace.getNodesAfter(workspaceNode.getFqn());

      for (Iterator it = tailMap.entrySet().iterator(); it.hasNext();)
      {
         WorkspaceNode toDelete = (WorkspaceNode) ((Map.Entry) it.next()).getValue();
         if (toDelete.getFqn().isChildOrEquals(nodeFqn))
         {
            if (debug) log.debug("marking node " + toDelete.getFqn() + " as deleted");
            toDelete.markAsDeleted(true);
         }
         else
         {
            break; // no more children, we came to the end
         }
      }
   }

   private Object removeKey(Object[] args, TransactionWorkspace workspace, WorkspaceNode workspaceNode)
   {
      if (workspaceNode == null)
         return null;
      Object removeKey = args[2];
      Object old = workspaceNode.remove(removeKey);
      workspace.addNode(workspaceNode);
      return old;
   }

   private void removeData(TransactionWorkspace workspace, WorkspaceNode workspaceNode)
   {
      if (workspaceNode == null)
         return;
      workspaceNode.clear();
      workspace.addNode(workspaceNode);
   }

   private Object getValueForKey(Object[] args, TransactionWorkspace workspace)
   {
      Fqn fqn = (Fqn) args[0];
      Object key = args[1];
      WorkspaceNode workspaceNode = getOrCreateWorkspaceNode(fqn, workspace, false);

      if (workspaceNode == null)
      {
         if (log.isDebugEnabled()) log.debug("unable to find node " + fqn + " in workspace.");
         return null;
      }
      else
      {
         //add this node into the wrokspace
         Object val = workspaceNode.get(key);
         workspace.addNode(workspaceNode);
         return val;
      }
   }

   private Object getNode(Object[] args, TransactionWorkspace workspace)
   {
      Fqn fqn = (Fqn) args[0];

      WorkspaceNode workspaceNode = getOrCreateWorkspaceNode(fqn, workspace, false);

      if (workspaceNode == null)
      {
         if (log.isDebugEnabled()) log.debug("unable to find node " + fqn + " in workspace.");
         return null;
      }
      else
      {
         workspace.addNode(workspaceNode);
         return workspaceNode.getNode();
      }
   }

   private Object getKeys(Object[] args, TransactionWorkspace workspace)
   {
      Fqn fqn = (Fqn) args[0];

      WorkspaceNode workspaceNode = getOrCreateWorkspaceNode(fqn, workspace, false);

      if (workspaceNode == null)
      {
         if (log.isDebugEnabled()) log.debug("unable to find node " + fqn + " in workspace.");
         return null;
      }
      else
      {
         Object keySet = workspaceNode.getKeys();
         workspace.addNode(workspaceNode);
         return keySet;
      }
   }

   private Object getChildNames(Object[] args, TransactionWorkspace workspace)
   {
      Fqn fqn = (Fqn) args[0];

      WorkspaceNode workspaceNode = getOrCreateWorkspaceNode(fqn, workspace, false);

      if (workspaceNode == null)
      {
         if (log.isDebugEnabled()) log.debug("unable to find node " + fqn + " in workspace.");
         return null;
      }
      else
      {
         Object nameSet = workspaceNode.getChildrenNames();
         workspace.addNode(workspaceNode);
         return nameSet;
      }
   }

   private WorkspaceNode getOrCreateWorkspaceNode(Fqn fqn, TransactionWorkspace workspace, boolean undeleteIfNecessary)
   {
      if (log.isTraceEnabled()) log.trace("Attempting to get node " + fqn + " into the workspace");
      WorkspaceNode workspaceNode = workspace.getNode(fqn);
      // if we do not have the node then we need to add it to the workspace
      if (workspaceNode == null)
      {
         DataNode node = cache.peek(fqn);
         if (node == null)
         {
            workspaceNode = null; // seems to happen quite a bit
         }
         else
         {
            workspaceNode = NodeFactory.getInstance().createWorkspaceNode(node, workspace);
            workspace.addNode(workspaceNode);
         }
      }
      // the node has been deleted dude!
      if (workspaceNode != null && workspaceNode.isDeleted())
      {
         if (log.isDebugEnabled()) log.debug("Node " + fqn + " has been deleted in the workspace.");
         if (undeleteIfNecessary)
         {
            workspaceNode.markAsDeleted(false);
            // re-add to parent
            WorkspaceNode parent = getOrCreateWorkspaceNode(fqn.getParent(), workspace, true);
            parent.addChild(workspaceNode);
         }
         else
         {
            workspaceNode = null;
         }
      }

      if (workspaceNode != null && !(workspaceNode.getVersion() instanceof DefaultDataVersion))
      {
         log.trace("Setting versioning to explicit");
         workspaceNode.setVersioningImplicit(false);
      }

      // now make sure all parents are in the wsp as well
      if (workspaceNode != null && !fqn.isRoot()) getOrCreateWorkspaceNode(fqn.getParent(), workspace, false);

      return workspaceNode;
   }
}
