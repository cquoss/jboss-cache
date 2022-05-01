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
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TreeCache;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jboss.cache.optimistic.WorkspaceNode;
import org.jgroups.blocks.MethodCall;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Locks nodes during transaction boundaries
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:stevew@jofti.com">Steve Woodcock (stevew@jofti.com)</a>
 */
public class OptimisticLockingInterceptor extends OptimisticInterceptor
{
    private long lockAcquisitionTimeout;

    public void setCache(TreeCache cache)
    {
        super.setCache(cache);
        lockAcquisitionTimeout = cache.getLockAcquisitionTimeout();
    }

    public Object invoke(MethodCall call) throws Throwable
    {
        JBCMethodCall m = (JBCMethodCall) call;
        InvocationContext ctx = getInvocationContext();
        Object retval = null;
        Method meth = m.getMethod();

       // bypass for buddy group org metod calls.
       if (isBuddyGroupOrganisationMethod(m)) return super.invoke(m);
       

        // bail out if _lock() is being called on the tree cache... this should never be called with o/l enabled.
        if (m.getMethodId() == MethodDeclarations.lockMethodLocal_id)
        {
            log.warn("OptimisticLockingInterceptor intercepted a call to TreeCache._lock().  " +
                "This should NEVER be called if optimistic locking is used!!  "+
                "Not allowing this call to proceed further down the chain.");
            return retval;
        }

        if (ctx.getTransaction() != null)
        {
            GlobalTransaction gtx = ctx.getGlobalTransaction();

            if (gtx == null)
            {
                throw new Exception("failed to get global transaction");
            }
            //we are interested in the prepare/commit/rollback

            //methods we are interested in are prepare/commit
            //this is irrespective of whether we are local or remote
            switch (m.getMethodId()) 
            {
               case MethodDeclarations.optimisticPrepareMethod_id:
                  //try and acquire the locks - before passing on
                  try
                  {
                      if (log.isDebugEnabled()) log.debug("Calling lockNodes() with gtx " + ctx.getGlobalTransaction());
                      lockNodes(gtx);
                  }
                  catch (Throwable e)
                  {
                      log.debug("Caught exception attempting to lock nodes ", e);
                      //we have failed - set to rollback and throw exception
                      try
                      {
                          unlock(gtx);
                      }
                      catch (Throwable t)
                      {
                          // we have failed to unlock - now what?
                          log.fatal("Failed to unlock on prepare ", t);
                      }
                      throw e;

                  }
                  // locks have acquired so lets pass on up
                  retval = super.invoke(m);
                  break;
               case MethodDeclarations.commitMethod_id:
               case MethodDeclarations.rollbackMethod_id:
                  // we need to let the stack run its commits first -
                  // we unlock last - even if an exception occurs
                  try
                  {
                      retval = super.invoke(m);
                      unlock(gtx);
                  }
                  catch (Throwable t)
                  {
                      log.debug("exception encountered on " + meth + " running unlock ", t);
                      try
                      {
                          unlock(gtx);
                      }
                      catch (Throwable ct)
                      {
                          log.fatal("Failed to unlock on " + meth, t);
                      }
                      throw t;
                  }
                  break;
               default :
                  //we do not care
                  retval = super.invoke(m);
                  break;
            }

        }
        else
        {
            throw new CacheException("not in a transaction");
        }

        return retval;
    }


    private void lockNodes(GlobalTransaction gtx) throws Exception
    {
        TransactionWorkspace workspace = getTransactionWorkspace(gtx);
        TransactionEntry te = cache.getTransactionTable().get(gtx);
        log.debug("locking nodes");

        // should be an ordered list
        Collection nodes = workspace.getNodes().values();

        for (Iterator it = nodes.iterator(); it.hasNext();)
        {
            WorkspaceNode workspaceNode = (WorkspaceNode) it.next();
            DataNode node = workspaceNode.getNode();

            boolean writeLock = workspaceNode.isDirty() || workspaceNode.isCreated() || workspaceNode.isDeleted() || (workspaceNode.isChildrenModified() && cache.getLockParentForChildInsertRemove());

            boolean acquired = node.acquire(gtx, lockAcquisitionTimeout, writeLock ? DataNode.LOCK_TYPE_WRITE : DataNode.LOCK_TYPE_READ);
            if (acquired)
            {
                if (log.isTraceEnabled()) log.trace("acquired lock on node " + node.getName());
                te.addLock(node.getLock());
            }
            else
            {
                throw new CacheException("unable to acquire lock on node " + node.getName());
            }
        }
    }


    private void unlock(GlobalTransaction gtx)
    {
        TransactionEntry entry = txTable.get(gtx);
        entry.releaseAllLocksFIFO(gtx);
    }

}
