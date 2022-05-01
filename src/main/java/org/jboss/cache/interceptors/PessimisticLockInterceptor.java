/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.interceptors;

import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TransactionTable;
import org.jboss.cache.TreeCache;
import org.jboss.cache.lock.IdentityLock;
import org.jboss.cache.lock.IsolationLevel;
import org.jboss.cache.lock.LockingException;
import org.jboss.cache.lock.TimeoutException;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Transaction;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An interceptor that handles locking. When a TX is associated, we register
 * for TX completion and unlock the locks acquired within the scope of the TX.
 * When no TX is present, we keep track of the locks acquired during the
 * current method and unlock when the method returns.
 *
 * @author Bela Ban
 * @version $Id: PessimisticLockInterceptor.java 5459 2008-03-25 17:52:13Z mircea.markus $
 */
public class PessimisticLockInterceptor extends Interceptor
{
   TransactionTable tx_table = null;

   boolean writeLockOnChildInsertRemove = true;

   /**
    * Map<Object, java.util.List>. Keys = threads, values = lists of locks held by that thread
    */
   Map lock_table;
   private long lock_acquisition_timeout;


   public void setCache(TreeCache cache)
   {
      super.setCache(cache);
      tx_table = cache.getTransactionTable();
      lock_table = cache.getLockTable();
      lock_acquisition_timeout = cache.getLockAcquisitionTimeout();
      writeLockOnChildInsertRemove = cache.getLockParentForChildInsertRemove();
   }


   public Object invoke(MethodCall call) throws Throwable
   {
      JBCMethodCall m = (JBCMethodCall) call;
      Fqn fqn = null;
      int lock_type = DataNode.LOCK_TYPE_NONE;
      long lock_timeout = lock_acquisition_timeout;
      Object[] args = m.getArgs();
      InvocationContext ctx = getInvocationContext();
      boolean storeLockedNode = false;

      if (log.isTraceEnabled()) log.trace("PessimisticLockInterceptor invoked for method " + m);
      if (ctx.getOptionOverrides() != null && ctx.getOptionOverrides().isSuppressLocking())
      {
         log.trace("Suppressing locking");
         switch (m.getMethodId())
         {
            case MethodDeclarations.putDataMethodLocal_id:
            case MethodDeclarations.putDataEraseMethodLocal_id:
            case MethodDeclarations.putKeyValMethodLocal_id:
            case MethodDeclarations.putFailFastKeyValueMethodLocal_id:
               log.trace("Creating nodes if necessary");
               createNodes((Fqn) args[1], ctx.getGlobalTransaction());
               break;
         }

         return super.invoke(m);
      }

      /** List<IdentityLock> locks. Locks acquired during the current method; will be released later by UnlockInterceptor.
       *  This list is only populated when there is no TX, otherwise the TransactionTable maintains the locks
       * (keyed by TX) */
      // List locks=null;

      boolean recursive = false;
      boolean createIfNotExists = false;
      boolean zeroLockTimeout = false; // only used if the call is an evict() call.  See JBCACHE-794
      boolean isRemoveData = false;

      // 1. Determine the type of lock (read, write, or none) depending on the method. If no lock is required, invoke
      //    the method, then return immediately
      //    Set the Fqn
      switch (m.getMethodId())
      {
         case MethodDeclarations.putDataMethodLocal_id:
         case MethodDeclarations.putDataEraseMethodLocal_id:
         case MethodDeclarations.putKeyValMethodLocal_id:
         case MethodDeclarations.putFailFastKeyValueMethodLocal_id:
            createIfNotExists = true;
            fqn = (Fqn) args[1];
            lock_type = DataNode.LOCK_TYPE_WRITE;
            if (m.getMethodId() == MethodDeclarations.putFailFastKeyValueMethodLocal_id)
               lock_timeout = ((Long) args[5]).longValue();
            break;
         case MethodDeclarations.removeNodeMethodLocal_id:
            fqn = (Fqn) args[1];
            lock_type = DataNode.LOCK_TYPE_WRITE;
            recursive = true; // remove node and *all* child nodes
            // BES 2007/12/12 -- Revert JBCACHE-1165 fix as it causes endless loop
            // in TransactionTest.testDoubleNodeRemoval, plus another failure 
            // in that test
//            createIfNotExists = true;
            // JBCACHE-871 We need to store the node
            storeLockedNode = true;
            break;
         case MethodDeclarations.removeKeyMethodLocal_id:
         case MethodDeclarations.removeDataMethodLocal_id:
            isRemoveData = true;
         case MethodDeclarations.addChildMethodLocal_id:
            fqn = (Fqn) args[1];
            lock_type = DataNode.LOCK_TYPE_WRITE;
            break;
         case MethodDeclarations.evictNodeMethodLocal_id:
            zeroLockTimeout = true;
            fqn = (Fqn) args[0];
            lock_type = DataNode.LOCK_TYPE_WRITE;
            break;
         case MethodDeclarations.getKeyValueMethodLocal_id:
         case MethodDeclarations.getNodeMethodLocal_id:
         case MethodDeclarations.getKeysMethodLocal_id:
         case MethodDeclarations.getChildrenNamesMethodLocal_id:
         case MethodDeclarations.releaseAllLocksMethodLocal_id:
         case MethodDeclarations.printMethodLocal_id:
            fqn = (Fqn) args[0];
            lock_type = DataNode.LOCK_TYPE_READ;
            break;
         case MethodDeclarations.lockMethodLocal_id:
            fqn = (Fqn) args[0];
            lock_type = ((Integer) args[1]).intValue();
            recursive = ((Boolean) args[2]).booleanValue();
            break;
         case MethodDeclarations.commitMethod_id:
            // commit propagated up from the tx interceptor
            commit(ctx.getGlobalTransaction());
            break;
         case MethodDeclarations.rollbackMethod_id:
            // rollback propagated up from the tx interceptor
            rollback(ctx.getGlobalTransaction());
            break;
         default:
            if (isOnePhaseCommitPrepareMehod(m))
            {
               // commit propagated up from the tx interceptor
               commit(ctx.getGlobalTransaction());
            }
            break;
      }

      // Lock the node (must be either read or write if we get here)
      // If no TX: add each acquired lock to the list of locks for this method (locks)
      // If TX: [merge code from TransactionInterceptor]: register with TxManager, on commit/rollback,
      // release the locks for the given TX
      if (fqn != null)
      {
         long timeout = zeroLockTimeout ? 0 : lock_acquisition_timeout;
         // make sure we can bail out of this loop
         long cutoffTime = System.currentTimeMillis() + timeout;
         boolean firstTry = true;
         if (createIfNotExists)
         {
            do
            {
               if (!firstTry && System.currentTimeMillis() > cutoffTime)
               {
                  throw new TimeoutException("Unable to acquire lock on Fqn " + fqn + " after " + timeout + " millis");
               }
               lock(fqn, ctx.getGlobalTransaction(), lock_type, recursive, zeroLockTimeout ? 0 : lock_timeout, createIfNotExists, storeLockedNode, isRemoveData);
               firstTry = false;
            }
            while(!cache.exists(fqn)); // keep trying until we have the lock (fixes concurrent remove())
            // terminates successfully, or with (Timeout)Exception
         }
         else
            lock(fqn, ctx.getGlobalTransaction(), lock_type, recursive, zeroLockTimeout ? 0 : lock_timeout, createIfNotExists, storeLockedNode, isRemoveData);
      }
      else
      {
         if (log.isTraceEnabled())
            log.trace("bypassed locking as method " + m.getName() + "() doesn't require locking");
      }
      if (m.getMethodId() == MethodDeclarations.lockMethodLocal_id)
         return null;

      Object o = super.invoke(m);

      // FIXME this should be done in UnlockInterceptor, but I didn't want
      // to add the removedNodes map to TreeCache
      if (storeLockedNode && ctx.getGlobalTransaction() == null)
      {
         // do a REAL remove here.
         // this is for NON TRANSACTIONAL calls
         cache.realRemove(fqn, true);
      }
      else if (m.getMethodId() == MethodDeclarations.commitMethod_id || isOnePhaseCommitPrepareMehod(m) || m.getMethodId() == MethodDeclarations.rollbackMethod_id)
      {
         // and this is for transactional ones
         cleanup(ctx.getGlobalTransaction());
      }

      return o;
   }

   private void cleanup(GlobalTransaction gtx)
   {
      TransactionEntry entry = tx_table.get(gtx);
      // Let's do it in stack style, LIFO
      entry.releaseAllLocksLIFO(gtx);

      Transaction ltx = entry.getTransaction();
      if (log.isTraceEnabled())
      {
         log.trace("removing local transaction " + ltx + " and global transaction " + gtx);
      }
      tx_table.remove(ltx);
      tx_table.remove(gtx);
   }

   /**
    * Locks a given node.
    *
    * @param fqn
    * @param gtx
    * @param lock_type DataNode.LOCK_TYPE_READ, DataNode.LOCK_TYPE_WRITE or DataNode.LOCK_TYPE_NONE
    * @param recursive Lock children recursively
    */
   private void lock(Fqn fqn, GlobalTransaction gtx, int lock_type, boolean recursive,
                     long lock_timeout, boolean createIfNotExists, boolean isRemoveNodeOperation, boolean isRemoveDataOperation)
           throws TimeoutException, LockingException, InterruptedException
   {
      DataNode n;
      DataNode child_node;
      Object child_name;
      Thread currentThread = Thread.currentThread();
      Object owner = (gtx != null) ? (Object) gtx : currentThread;
      int treeNodeSize;
      int currentLockType;


      if (log.isTraceEnabled()) log.trace("Attempting to lock node " + fqn + " for owner " + owner);

      if (fqn == null)
      {
         log.error("fqn is null - this should not be the case");
         return;
      }

      if ((treeNodeSize = fqn.size()) == 0)
         return;

      if (cache.getIsolationLevelClass() == IsolationLevel.NONE)
         lock_type = DataNode.LOCK_TYPE_NONE;

      n = cache.getRoot();
      for (int i = -1; i < treeNodeSize; i++)
      {
         if (i == -1)
         {
            child_name = Fqn.ROOT.getName();
            child_node = cache.getRoot();
         }
         else
         {
            child_name = fqn.get(i);
            child_node = (DataNode) n.getOrCreateChild(child_name, gtx, createIfNotExists);
         }

         if (child_node == null)
         {
            if (log.isTraceEnabled())
               log.trace("failed to find or create child " + child_name + " of node " + n.getFqn());
            return;
         }

         if (lock_type == DataNode.LOCK_TYPE_NONE)
         {
            // acquired=false;
            n = child_node;
            continue;
         }
         else
         {
            if (writeLockNeeded(lock_type, i, treeNodeSize, isRemoveNodeOperation, createIfNotExists, isRemoveDataOperation, fqn, child_node.getFqn()))
            {
               currentLockType = DataNode.LOCK_TYPE_WRITE;
            }
            else
            {
               currentLockType = DataNode.LOCK_TYPE_READ;
            }
         }

         // reverse the "remove" if the node has been previously removed in the same tx, if this operation is a put()
         if (gtx != null && needToReverseRemove(child_node, tx_table.get(gtx), lock_type, isRemoveNodeOperation, createIfNotExists))
         {
            reverseRemove(child_node);
         }


         // Try to acquire the lock; recording that we did if successful
         acquireNodeLock(child_node, owner, gtx, currentLockType, lock_timeout);
         
         // BES 2007/12/12 -- Revert JBCACHE-1165 fix as it causes endless loop
         // in TransactionTest.testDoubleNodeRemoval, plus another failure 
         // in that test
//         // make sure the lock we acquired isn't on a deleted node/is an orphan!!
//         DataNode repeek = cache.peek(child_node.getFqn());
//         if (repeek != null && child_node != repeek)
//         {
//            log.trace("Was waiting for and obtained a lock on a node that doesn't exist anymore!  Attempting lock acquisition again.");
//            // we have an orphan!! Lose the unnecessary lock and re-acquire the lock (and potentially recreate the node).
//            child_node.getLock().release(owner);
//
//            // do the loop again, but don't assign child_node to n so that child_node is processed again.
//            i--;
//            continue;
//         }

         if (recursive && isTargetNode(i, treeNodeSize))
         {
            {
               Set acquired_locks = child_node.acquireAll(owner, lock_timeout, lock_type);
               if (acquired_locks.size() > 0)
               {
                  if (gtx != null)
                  {
                     cache.getTransactionTable().addLocks(gtx, acquired_locks);
                  }
                  else
                  {
                     List locks = getLocks(Thread.currentThread());
                     locks.addAll(acquired_locks);
                  }
               }
            }
         }
         n = child_node;
      }

      // Add the Fqn to be removed to the transaction entry so we can clean up after ourselves during commit/rollback
      if (isRemoveNodeOperation && gtx != null) cache.getTransactionTable().get(gtx).addRemovedNode(fqn);
   }

   private boolean needToReverseRemove(DataNode n, TransactionEntry te, int lockTypeRequested, boolean isRemoveOperation, boolean createIfNotExists)
   {
      return !isRemoveOperation && createIfNotExists && lockTypeRequested == DataNode.LOCK_TYPE_WRITE && n.isMarkedForRemoval()
              && hasBeenRemovedInCurrentTx(te, n.getFqn());
   }

   private boolean hasBeenRemovedInCurrentTx(TransactionEntry te, Fqn f)
   {
      if (te.getRemovedNodes().contains(f)) return true;

      Iterator i = te.getRemovedNodes().iterator();
      while (i.hasNext())
      {
         Fqn removed = (Fqn) i.next();
         if (f.isChildOf(removed)) return true;
      }
      return false;
   }

   private void reverseRemove(DataNode n)
   {
      n.unmarkForRemoval(false);
   }

   private boolean writeLockNeeded(int lock_type, int currentNodeIndex, int treeNodeSize, boolean isRemoveOperation, boolean isPutOperation, boolean isRemoveDataOperation, Fqn targetFqn, Fqn currentFqn)
   {
      if (writeLockOnChildInsertRemove)
      {
         if (isRemoveOperation && currentNodeIndex == treeNodeSize - 2)
            return true; // we're doing a remove and we've reached the PARENT node of the target to be removed.

         if (!isTargetNode(currentNodeIndex, treeNodeSize) && !cache.exists(new Fqn(currentFqn, targetFqn.get(currentNodeIndex + 1))))
            return isPutOperation; // we're at a node in the tree, not yet at the target node, and we need to create the next node.  So we need a WL here.
      }

      return lock_type == DataNode.LOCK_TYPE_WRITE && isTargetNode(currentNodeIndex, treeNodeSize) && (isPutOperation || isRemoveOperation || isRemoveDataOperation); //normal operation, write lock explicitly requested and this is the target to be written to.
   }

   private boolean isTargetNode(int nodePosition, int treeNodeSize)
   {
      return nodePosition == (treeNodeSize - 1);
   }

   private void acquireNodeLock(DataNode node, Object owner, GlobalTransaction gtx, int lock_type, long lock_timeout) throws LockingException, TimeoutException, InterruptedException
   {
      boolean acquired = node.acquire(owner, lock_timeout, lock_type);
      if (acquired)
      {
         // Record the lock for release on method return or tx commit/rollback
         recordNodeLock(gtx, node.getLock());
      }
   }

   private void recordNodeLock(GlobalTransaction gtx, IdentityLock lock)
   {
      if (gtx != null)
      {
         // add the lock to the list of locks maintained for this transaction
         // (needed for release of locks on commit or rollback)
         cache.getTransactionTable().addLock(gtx, lock);
      }
      else
      {
         List locks = getLocks(Thread.currentThread());
         if (!locks.contains(lock))
            locks.add(lock);
      }
   }


   private List getLocks(Thread currentThread)
   {
      // This sort of looks like a get/put race condition, but
      // since we key off the Thread, it's not
      List locks = (List) lock_table.get(currentThread);
      if (locks == null)
      {
         locks = Collections.synchronizedList(new LinkedList());
         lock_table.put(currentThread, locks);
      }
      return locks;
   }


   private void createNodes(Fqn fqn, GlobalTransaction gtx)
   {
      int treeNodeSize;
      if ((treeNodeSize = fqn.size()) == 0) return;
      DataNode n = cache.getRoot();
      for (int i = 0; i < treeNodeSize; i++)
      {
         Object child_name = fqn.get(i);
         DataNode child_node = (DataNode) n.getOrCreateChild(child_name, gtx, true);
         // test if this node needs to be 'undeleted'
         // reverse the "remove" if the node has been previously removed in the same tx, if this operation is a put()
         if (gtx != null && needToReverseRemove(child_node, tx_table.get(gtx), DataNode.LOCK_TYPE_WRITE, false, true))
         {
            reverseRemove(child_node);
         }

         if (child_node == null)
         {
            if (log.isTraceEnabled())
               log.trace("failed to find or create child " + child_name + " of node " + n.getFqn());
            return;
         }
         n = child_node;
      }
   }


   /**
    * Remove all locks held by <tt>tx</tt>, remove the transaction from the transaction table
    *
    * @param gtx
    */
   private void commit(GlobalTransaction gtx)
   {
      if (log.isTraceEnabled())
         log.trace("committing cache with gtx " + gtx);

      TransactionEntry entry = tx_table.get(gtx);
      if (entry == null)
      {
         log.error("entry for transaction " + gtx + " not found (maybe already committed)");
         return;
      }

      // first remove nodes that should be deleted.
      Iterator removedNodes = entry.getRemovedNodes().iterator();
      while (removedNodes.hasNext())
      {
         Fqn f = (Fqn) removedNodes.next();
         cache.realRemove(f, false);
      }
   }


   /**
    * Revert all changes made inside this TX: invoke all method calls of the undo-ops
    * list. Then release all locks and remove the TX from the transaction table.
    * <ol>
    * <li>Revert all modifications done in the current TX<li/>
    * <li>Release all locks held by the current TX</li>
    * <li>Remove all temporary nodes created by the current TX</li>
    * </ol>
    *
    * @param tx
    */
   private void rollback(GlobalTransaction tx)
   {
      TransactionEntry entry = tx_table.get(tx);

      if (log.isTraceEnabled())
         log.trace("called to rollback cache with GlobalTransaction=" + tx);

      if (entry == null)
      {
         log.error("entry for transaction " + tx + " not found (transaction has possibly already been rolled back)");
         return;
      }

      Iterator removedNodes = entry.getRemovedNodes().iterator();
      while (removedNodes.hasNext())
      {
         Fqn f = (Fqn) removedNodes.next();
         cache.realRemove(f, false);

      }

      // Revert the modifications by running the undo-op list in reverse. This *cannot* throw any exceptions !
      entry.undoOperations(cache);
   }

}
