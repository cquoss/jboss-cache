/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.interceptors;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TreeCache;
import org.jboss.cache.buddyreplication.BuddyManager;
import org.jboss.cache.config.Option;
import org.jboss.cache.loader.NodeData;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.invocation.MarshalledValueInputStream;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * The Data Gravitator interceptor intercepts cache misses and attempts t gravitate data from other parts of the cluster.
 * <p/>
 * Only used if Buddy Replication is enabled.  Also, the interceptor only kicks in if an {@link Option} is passed in to
 * force Data Gravitation for a specific invocation or if <b>autoDataGravitation</b> is set to <b>true</b> when configuring
 * Buddy Replication.
 * <p/>
 * See the JBoss Cache User Guide for more details on configuration options.  There is a section dedicated to Buddy Replication
 * in the Replication chapter.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class DataGravitatorInterceptor extends BaseRpcInterceptor
{
   private BuddyManager buddyManager;
   private boolean syncCommunications = false;
   private Log log = LogFactory.getLog(DataGravitatorInterceptor.class);
   private Map transactionMods = new ConcurrentHashMap();

   public void setCache(TreeCache cache)
   {
      super.setCache(cache);
      this.buddyManager = cache.getBuddyManager();
      syncCommunications = cache.getCacheModeInternal() == TreeCache.REPL_SYNC;
   }

   public Object invoke(MethodCall call) throws Throwable
   {
      JBCMethodCall m = (JBCMethodCall) call;
      InvocationContext ctx = getInvocationContext();

      if (((ctx.getOptionOverrides() != null) && ctx.getOptionOverrides().isSkipDataGravitation()))
      {
         return super.invoke(call);
      }

      if (log.isTraceEnabled()) log.trace("Invoked with method call " + m);

      // Transactional lifecycle methods should be handled regardless of whether data gravitation is enabled or not.
      if (!isTransactionLifecycleMethod(m))
      {
         if (isGravitationEnabled(ctx) && MethodDeclarations.isGetMethod(m.getMethodId()))
         {
            // test that the Fqn being requested exists locally in the cache.
            Fqn fqn = extractFqn(m.getMethodId(), m.getArgs());
            if (log.isTraceEnabled()) log.trace("Checking local existence of fqn " + fqn);
            if (BuddyManager.isBackupFqn(fqn))
            {
               log.info("Is call for a backup Fqn, not performing any gravitation.  Direct calls on internal backup nodes are *not* supported.");
            }
            else
            {
               if (!cache.exists(fqn))
               {
                  // perform a data gravitation
                  log.trace("Gravitating from local backup tree");
                  BackupData data = localBackupGet(fqn);
                  
                  if (data == null)
                  {
                     log.trace("Gravitating from remote backup tree");
                     // gravitate remotely.
                     data = remoteBackupGet(fqn);
                  }

                  if (data != null)
                  {
                     // create node locally so I don't gravitate again
                     // when I do the put() call to the cluster!
                     createNode(true, data.backupData);
                     // Make sure I replicate to my buddies.
                     log.trace("Passing the put call locally to make sure state is persisted and ownership is correctly established.");
                     createNode(false, data.backupData);

                     // now obtain locks -
                     lock(data.primaryFqn);

                     // Clean up the other nodes
                     cleanBackupData(data);
                  }
               }
            }
         }
         else
         {
            if (log.isTraceEnabled())
               log.trace("Suppressing data gravitation for this call.");
         }
      }
      else
      {

         try
         {
            switch (m.getMethodId())
            {
               case MethodDeclarations.prepareMethod_id:
               case MethodDeclarations.optimisticPrepareMethod_id:
                  Object o = super.invoke(m);
                  doPrepare(ctx.getGlobalTransaction());
                  return o;
               case MethodDeclarations.rollbackMethod_id:
                  transactionMods.remove(ctx.getGlobalTransaction());
                  return super.invoke(m);
               case MethodDeclarations.commitMethod_id:
                  doCommit(ctx.getGlobalTransaction());
                  transactionMods.remove(ctx.getGlobalTransaction());
                  return super.invoke(m);
            }
         }
         catch (Throwable throwable)
         {
            transactionMods.remove(ctx.getGlobalTransaction());
            throw throwable;
         }
      }
//            }
//        }
//        else
//        {
//           if (log.isTraceEnabled())
//              log.trace("Suppressing data gravitation for this call.");
//        }
      return super.invoke(m);
   }

   protected void lock(Fqn fqn) throws Throwable
   {

      if (cache.isNodeLockingOptimistic()) return;

      MethodCall meth = MethodCallFactory.create(MethodDeclarations.lockMethodLocal,
              new Object[]{fqn,
                      new Integer(DataNode.LOCK_TYPE_WRITE),
                      Boolean.FALSE});
      //super.invoke(meth);
      // TxInterceptor will scrub the InvocationContext of any Option, so
      // cache it so we can restore it
      InvocationContext ctx = getInvocationContext();
      Option opt = ctx.getOptionOverrides();
      try
      {
         ((Interceptor) cache.getInterceptors().get(0)).invoke(meth); // need a better way to do this
      }
      finally
      {
         if (opt != null)
            ctx.setOptionOverrides(opt);
      }
   }

   private boolean isGravitationEnabled(InvocationContext ctx)
   {
      boolean enabled = ctx.isOriginLocal();
      if (enabled)
      {
         if (!buddyManager.isAutoDataGravitation())
         {
            Option opt = ctx.getOptionOverrides();
            enabled = (opt != null && opt.getForceDataGravitation());
         }
      }
      return enabled;
   }

   private void doPrepare(GlobalTransaction gtx) throws Throwable
   {
      MethodCall cleanup = (MethodCall) transactionMods.get(gtx);
      if (log.isTraceEnabled()) log.trace("Broadcasting prepare for cleanup ops " + cleanup);
      if (cleanup != null)
      {
         JBCMethodCall prepare;
         List mods = new ArrayList(1);
         mods.add(cleanup);
         if (cache.isNodeLockingOptimistic())
         {
            prepare = MethodCallFactory.create(MethodDeclarations.optimisticPrepareMethod, new Object[]{gtx, mods, null, cache.getLocalAddress(), Boolean.FALSE});
         }
         else
         {
            prepare = MethodCallFactory.create(MethodDeclarations.prepareMethod, new Object[]{gtx, mods, cache.getLocalAddress(), cache.getCacheModeInternal() == TreeCache.REPL_SYNC || cache.getCacheModeInternal() == TreeCache.INVALIDATION_SYNC ? Boolean.FALSE : Boolean.TRUE});
         }

         replicateCall(getMembersOutsideBuddyGroup(), prepare, syncCommunications);
      }
      else
      {
         if (log.isTraceEnabled()) log.trace("Nothing to broadcast in prepare phase for gtx " + gtx);
      }
   }

   private void doCommit(GlobalTransaction gtx) throws Throwable
   {
      if (transactionMods.containsKey(gtx))
      {
         if (log.isTraceEnabled()) log.trace("Broadcasting commit for gtx " + gtx);
         replicateCall(getMembersOutsideBuddyGroup(), MethodCallFactory.create(MethodDeclarations.commitMethod, new Object[]{gtx}), syncCommunications);
      }
      else
      {
         if (log.isTraceEnabled()) log.trace("Nothing to broadcast in commit phase for gtx " + gtx);
      }
   }

   private List getMembersOutsideBuddyGroup()
   {
      List members = new ArrayList(cache.getMembers());
      members.remove(cache.getLocalAddress());
      members.removeAll(buddyManager.getBuddyAddresses());
      return members;
   }

   private BackupData remoteBackupGet(Fqn name) throws Exception
   {

      BackupData result = null;

      Object[] resp = gravitateData(name);

      if (resp[0] != null)
      {
         if (log.isTraceEnabled())
            log.trace("Got response " + resp[0]);

         List nodes = null;
         if (cache.getUseRegionBasedMarshalling())
         {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            try
            {
               cache.getRegionManager().setUnmarshallingClassLoader(name);

               byte[] nodeData = (byte[]) resp[0];
               ByteArrayInputStream bais = new ByteArrayInputStream(nodeData);
               MarshalledValueInputStream mais = new MarshalledValueInputStream(bais);
               nodes = (List) mais.readObject();
               mais.close();
            }
            finally
            {
               Thread.currentThread().setContextClassLoader(cl);
            }
         }
         else
         {
            nodes = (List) resp[0];
         }

         Fqn bkup = (Fqn) resp[1];
         result = new BackupData(name, bkup, nodes);
      }

      return result;
   }

   private void cleanBackupData(BackupData backup) throws Throwable
   {
//       MethodCall primaryDataCleanup, backupDataCleanup;
//       if (buddyManager.isDataGravitationRemoveOnFind())
//       {
//           primaryDataCleanup = MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal, new Object[]{null, backup.primaryFqn, Boolean.FALSE});
//           backupDataCleanup = MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal, new Object[]{null, backup.backupFqn, Boolean.FALSE});
//       }
//       else
//       {
//           primaryDataCleanup = MethodCallFactory.create(MethodDeclarations.evictNodeMethodLocal, new Object[]{backup.primaryFqn});
//           backupDataCleanup = MethodCallFactory.create(MethodDeclarations.evictNodeMethodLocal, new Object[]{backup.backupFqn});
//       }

      JBCMethodCall cleanup = MethodCallFactory.create(MethodDeclarations.dataGravitationCleanupMethod, new Object[]{getInvocationContext().getGlobalTransaction(), backup.primaryFqn, backup.backupFqn});


      if (log.isTraceEnabled()) log.trace("Performing cleanup on [" + backup.primaryFqn + "]");
      GlobalTransaction gtx = getInvocationContext().getGlobalTransaction();
      if (gtx == null)
      {
         // broadcast removes
         // remove main Fqn
         //replicateCall(cache.getMembers(), primaryDataCleanup, syncCommunications);

         if (log.isTraceEnabled()) log.trace("Performing cleanup on [" + backup.backupFqn + "]");
         // remove backup Fqn
         //replicateCall(cache.getMembers(), backupDataCleanup, syncCommunications);
         replicateCall(cache.getMembers(), cleanup, syncCommunications);
      }
      else
      {
         if (log.isTraceEnabled())
            log.trace("Data gravitation performed under global transaction " + gtx + ".  Not broadcasting cleanups until the tx commits.  Adding to tx mod list instead.");
         transactionMods.put(gtx, cleanup);
         TransactionEntry te = getTransactionEntry(gtx);
         te.addModification(cleanup);
      }
   }

   private Object[] gravitateData(Fqn fqn) throws Exception
   {
      if (log.isTraceEnabled())
         log.trace("cache=" + cache.getLocalAddress() + "; requesting data gravitation for Fqn " + fqn);
      Vector mbrs = cache.getMembers();
      Boolean searchSubtrees = (buddyManager.isDataGravitationSearchBackupTrees() ? Boolean.TRUE : Boolean.FALSE);
      Boolean marshal = cache.getUseRegionBasedMarshalling() ? Boolean.TRUE : Boolean.FALSE;
      MethodCall dGrav = MethodCallFactory.create(MethodDeclarations.dataGravitationMethod, new Object[]{fqn, searchSubtrees, marshal});
      // JBCACHE-1194 This must be GET_ALL, not GET_FIRST
      List resps = cache.callRemoteMethods(mbrs, dGrav, GroupRequest.GET_ALL, true, buddyManager.getBuddyCommunicationTimeout());
      if (resps == null)
      {
         if (mbrs.size() > 1) log.error("No replies to call " + dGrav);
         return new Object[]{null, null};
      }
      else
      {
         // test for and remove exceptions
         Iterator i = resps.iterator();
         Object result = null;
         Object backupFqn = null;

         while (i.hasNext())
         {
            Object o = i.next();
            if (o instanceof Throwable)
            {
               if (log.isDebugEnabled())
                  log.debug("Found remote Throwable among responses - removing from responses list", (Exception) o);
            }
            else if (o != null)
            {
               // keep looping till we find a FOUND answer.
               List dGravResp = (List) o;
               // found?
               if (((Boolean) dGravResp.get(0)).booleanValue())
               {
                  result = dGravResp.get(1);
                  backupFqn = dGravResp.get(2);
                  break;
               }
            }
            else if (!cache.getUseRegionBasedMarshalling())
            {
               // Null is OK if we are using region based marshalling; it
               // is what is returned if a region is inactive. Otherwise
               // getting a null is an error condition
               log.error("Unexpected null response to call " + dGrav + ".");
            }

         }

         if (log.isTraceEnabled()) log.trace("got responses " + resps);
         return new Object[]{result, backupFqn};
      }
   }

   private void createNode(boolean localOnly, List nodeData) throws CacheException
   {
      Iterator nodes = nodeData.iterator();
      GlobalTransaction gtx = getInvocationContext().getGlobalTransaction();

      while (nodes.hasNext())
      {
         NodeData data = (NodeData) nodes.next();
         if (localOnly)
         {
            if (!cache.exists(data.getFqn()))
            {
               createNodes(gtx, data.getFqn(), data.getAttributes());
            }
         }
         else
         {
            cache.put(data.getFqn(), data.getAttributes());
         }
      }
   }

   private void createNodes(GlobalTransaction gtx, Fqn fqn, Map data) throws CacheException
   {
      int treeNodeSize;
      if ((treeNodeSize = fqn.size()) == 0) return;
      DataNode n = cache.getRoot();
      for (int i = 0; i < treeNodeSize; i++)
      {
         Object child_name = fqn.get(i);
         DataNode child_node = (DataNode) n.getOrCreateChild(child_name, gtx, true);
         if (child_node == null)
         {
            if (log.isTraceEnabled())
               log.trace("failed to find or create child " + child_name + " of node " + n.getFqn());
            return;
         }
         if (i == treeNodeSize - 1)
         {
            // set data
            cache._put(gtx, fqn, data, true);
         }
         n = child_node;
      }
   }

   private TransactionEntry getTransactionEntry(GlobalTransaction gtx)
   {
      return cache.getTransactionTable().get(gtx);
   }

   private Fqn extractFqn(int methodId, Object[] args)
   {
      return (Fqn) args[MethodDeclarations.isCrudMethod(methodId) ? 1 : 0];
   }

   private boolean localBackupExists(Fqn fqn)
   {
      Iterator backupRoots = getBackupRootIterator();
      boolean exists = false;

      while (backupRoots.hasNext())
      {
         DataNode node = (DataNode) backupRoots.next();
         Fqn newSearchFqn = new Fqn(node.getFqn(), fqn);
         exists = cache.exists(newSearchFqn);
         if (exists) break;
      }

      return exists;
   }

   private BackupData localBackupGet(Fqn fqn) throws CacheException
   {
      List gravitatedData = cache._gravitateData(fqn, true, false); // a "local" gravitation
      boolean found = ((Boolean) gravitatedData.get(0)).booleanValue();
      BackupData data = null;

      if (found)
      {
         Fqn backupFqn = (Fqn) gravitatedData.get(2);
         List nodeData = (List) gravitatedData.get(1);
         data = new BackupData(fqn, backupFqn, nodeData);
         // now the cleanup
         if (buddyManager.isDataGravitationRemoveOnFind())
         {
            // Remove locally only; the remote call will
            // be broadcast later
            Option opt = new Option();
            opt.setCacheModeLocal(true);
            cache.remove(backupFqn, opt);
         }
         else
         {
            cache.evict(backupFqn);
         }
      }

      return data;
   }

   private Iterator getBackupRootIterator()
   {
      DataNode backupRoot = cache.peek(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN);
      return backupRoot == null ? Collections.EMPTY_SET.iterator() : backupRoot.getChildren().values().iterator();
   }

   private class BackupData
   {
      Fqn primaryFqn;
      Fqn backupFqn;
      List backupData;

      BackupData(Fqn primary, Fqn backup, List data)
      {
         this.primaryFqn = primary;
         this.backupFqn = backup;
         this.backupData = data;
      }
   }


}
