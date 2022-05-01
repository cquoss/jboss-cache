/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.interceptors;

import org.jboss.cache.*;
import org.jboss.cache.config.Option;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jgroups.blocks.MethodCall;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.lang.reflect.Method;
import java.util.*;

/**
 * This interceptor acts as a replacement to the replication interceptor when
 * the TreeCache is configured with ClusteredSyncMode as INVALIDATE.
 *
 * The idea is that rather than replicating changes to all caches in a cluster
 * when CRUD (Create, Remove, Update, Delete) methods are called, simply call
 * evict(Fqn) on the remote caches for each changed node.  This allows the
 * remote node to look up the value in a shared cache loader which would have
 * been updated with the changes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class InvalidationInterceptor extends BaseRpcInterceptor implements InvalidationInterceptorMBean
{
    private boolean synchronous;
    private long m_invalidations = 0;
    protected TransactionTable txTable;

    public void setCache(TreeCache cache)
    {
        super.setCache(cache);
        // may as well cache this test ...
        synchronous = cache.getCacheModeInternal() == TreeCache.INVALIDATION_SYNC;
        txTable=cache.getTransactionTable();
    }

    public Object invoke(MethodCall call) throws Throwable
    {
        JBCMethodCall m = (JBCMethodCall) call;
        InvocationContext ctx = getInvocationContext();
        Option optionOverride = ctx.getOptionOverrides();
        if (optionOverride != null && optionOverride.isCacheModeLocal() && ctx.getTransaction() == null)
        {
            // skip replication!!
            return super.invoke(m);
        }
        
        Transaction tx = ctx.getTransaction();
        Object retval = super.invoke(m);
        Method meth = m.getMethod();

        if (log.isTraceEnabled()) log.trace("(" + cache.getLocalAddress() + ") method call " + m );

        // now see if this is a CRUD method:
        if (MethodDeclarations.isCrudMethod(meth))
        {
            if (m.getMethodId() != MethodDeclarations.putFailFastKeyValueMethodLocal_id)
            {
               if (log.isDebugEnabled()) log.debug("Is a CRUD method");
               Fqn fqn = findFqn( m.getArgs() );
               if (fqn != null)
               {
                   // could be potentially TRANSACTIONAL.  Ignore if it is, until we see a prepare().
                   if (tx == null || !isValid(tx))
                   {
                       // the no-tx case:
                       //replicate an evict call.
                       invalidateAcrossCluster( fqn, null );
                   }
               }
            }
            else
            {
               log.debug("Encountered a putFailFast() - is a no op.");
            }
        }
        else
        {
            // not a CRUD method - lets see if it is a tx lifecycle method.
            if (tx != null && isValid(tx))
            {
                // lets see if we are in the prepare phase (as this is the only time we actually do anything)
               switch (m.getMethodId())
               {
                  case MethodDeclarations.prepareMethod_id:
                  case MethodDeclarations.optimisticPrepareMethod_id:
                     log.debug("Entering InvalidationInterceptor's prepare phase");
                     // fetch the modifications before the transaction is committed (and thus removed from the txTable)
                     GlobalTransaction gtx = ctx.getGlobalTransaction();
                     TransactionEntry entry = txTable.get(gtx);
                     if (entry == null) throw new IllegalStateException("cannot find transaction entry for " + gtx);
                     List modifications = new LinkedList(entry.getModifications());

                     if (modifications.size() > 0)
                     {
                        if (containsPutFailFast(modifications))
                        {
                           log.debug("Modification list contains a putFailFast operation.  Not invalidating.");
                        }
                        else
                        {
                            try
                            {
                                invalidateModifications(modifications, cache.isNodeLockingOptimistic() ? getWorkspace(gtx) : null);
                            }
                            catch (Throwable t)
                            {
                                log.warn("Unable to broadcast evicts as a part of the prepare phase.  Rolling back.", t);
                                try
                                {
                                    tx.setRollbackOnly();
                                }
                                catch (SystemException se)
                                {
                                    throw new RuntimeException("setting tx rollback failed ", se);
                                }
                                if (t instanceof RuntimeException)
                                    throw (RuntimeException) t;
                                else
                                    throw new RuntimeException("Unable to broadcast invalidation messages", t);
                            }
                        }
                     }
                     else
                     {
                        log.debug("Nothing to invalidate - no modifications in the transaction.");
                     }
                     break;
               }
            }

        }
        return retval;
    }

   private boolean containsPutFailFast(List l)
   {
      for (Iterator i = l.iterator(); i.hasNext();) if (((JBCMethodCall) i.next()).getMethodId() == MethodDeclarations.putFailFastKeyValueMethodLocal_id) return true;
      return false;
   }
    public long getInvalidations()
    {
        return m_invalidations;  
    }
    
    public void resetStatistics()
    {
        m_invalidations = 0;
    }
    
    public Map dumpStatistics()
    {
        Map retval=new HashMap();
        retval.put("Invalidations", new Long(m_invalidations));
        return retval;
    }

    protected void invalidateAcrossCluster(Fqn fqn, TransactionWorkspace workspace) throws Throwable
    {
        // increment invalidations counter if statistics maintained
        if (cache.getUseInterceptorMbeans()&& statsEnabled)
            m_invalidations++;
          
        // only propagate version details if we're using explicit versioning.
        JBCMethodCall call = workspace != null && !workspace.isVersioningImplicit() ?
            MethodCallFactory.create(MethodDeclarations.evictVersionedNodeMethodLocal, new Object[]{fqn, workspace.getNode(fqn).getVersion()}) :
            MethodCallFactory.create(MethodDeclarations.evictNodeMethodLocal, new Object[]{fqn});

        if (log.isDebugEnabled()) log.debug("Cache ["+cache.getLocalAddress()+"] replicating " + call);
        // voila, invalidated!
        replicateCall(call, synchronous);
    }

    protected void invalidateModifications(List modifications, TransactionWorkspace workspace) throws Throwable
    {
        // optimise the calls list here.
        Iterator modifiedFqns = optimisedIterator(modifications);
        while (modifiedFqns.hasNext())
        {
            Fqn fqn = (Fqn) modifiedFqns.next();
            invalidateAcrossCluster(fqn, workspace);
        }
    }

    protected TransactionWorkspace getWorkspace(GlobalTransaction gtx)
    {
        OptimisticTransactionEntry entry = (OptimisticTransactionEntry) txTable.get(gtx);
        return entry.getTransactionWorkSpace();
    }

    protected Fqn findFqn(Object[] objects)
    {
        // it *should* be the 2nd param...
        return (Fqn) objects[1];
    }

    /**
     * Removes non-crud methods, plus clobs together common calls to Fqn's.
     * E.g, if we have put("/a/b", "1", "2") followed by a put("/a/b", "3",
     * "4") we should only evict "/a/b" once.
     * @param list
     * @return Iterator containing a unique set of Fqns of crud methods in this tx
     */
    protected Iterator optimisedIterator(List list)
    {
        Set fqns = new HashSet();
        Iterator listIter = list.iterator();
        while (listIter.hasNext())
        {
            MethodCall mc = (MethodCall) listIter.next();
            if (MethodDeclarations.isCrudMethod(mc.getMethod()))
            {
                fqns.add(findFqn(mc.getArgs()));
            }
        }
        return fqns.iterator();
    }
}
