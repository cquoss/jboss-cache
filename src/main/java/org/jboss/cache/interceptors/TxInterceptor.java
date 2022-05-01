/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.interceptors;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.jboss.cache.CacheException;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.OptimisticTransactionEntry;
import org.jboss.cache.ReplicationException;
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TransactionTable;
import org.jboss.cache.TreeCache;
import org.jboss.cache.config.Option;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.optimistic.DataVersion;
import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This interceptor is the new default at the head of all interceptor chains,
 * and makes transactional attributes available to all interceptors in the chain.
 * This interceptor is also responsible for registering for synchronisation on
 * transaction completion.
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:stevew@jofti.com">Steve Woodcock (stevew@jofti.com)</a>
 */
public class TxInterceptor extends OptimisticInterceptor implements TxInterceptorMBean
{
    /**
     * List <Transaction>that we have registered for
     */
    private Map transactions = new ConcurrentHashMap(16);
    private Map rollbackTransactions = new ConcurrentHashMap(16);
    private long m_prepares = 0;
    private long m_commits = 0;
    private long m_rollbacks = 0;
    final static Object NULL = new Object();
    protected TransactionManager txManager = null;
    protected TransactionTable txTable = null;


    /**
     * Set<GlobalTransaction> of GlobalTransactions that originated somewhere else (we didn't create them).
     * This is a result of a PREPARE phase. GlobalTransactions in this list should be ignored by this
     * interceptor when registering for TX completion
     */
    private Map remoteTransactions = new ConcurrentHashMap();


    public void setCache(TreeCache cache)
    {
        super.setCache(cache);
        txManager = cache.getTransactionManager();
        txTable = cache.getTransactionTable();
    }

    public Object invoke(MethodCall call) throws Throwable
    {
        JBCMethodCall m = (JBCMethodCall) call;
        if (log.isTraceEnabled())
        {
            log.trace("("+cache.getLocalAddress()+") call on method [" + m + "]");
        }
        // bypass for buddy group org metod calls.
        if (isBuddyGroupOrganisationMethod(m)) return super.invoke(m);

        InvocationContext ctx = getInvocationContext();

        final Transaction suspendedTransaction;
        boolean scrubTxsOnExit = false;
        final boolean resumeSuspended;
        Option optionOverride = ctx.getOptionOverrides();
       setTransactionInContext(ctx, txManager);

        if (optionOverride!= null && optionOverride.isFailSilently() && ctx.getTransaction() != null)
        {
           // JBCACHE-1246 If we haven't previously registered a synchronization 
           // for the current tx, remove it from the tx table, since we are about to
           // suspend it and thus won't remove it later via the synchronization.
           // If we don't do this, we leak the tx and gtx in txTable.
           // BES -- I'm using the transactions map here as a proxy for whether
           // the tx has had a synchronization registered. Not really ideal...
           if (transactions.get(ctx.getTransaction()) == null)
           {
              // make sure we remove the tx and global tx from the transaction table, since we don't care about this transaction
              // and will just suspend it.  - JBCACHE-1246
              GlobalTransaction gtx = txTable.remove(ctx.getTransaction());
              if (gtx != null) txTable.remove(gtx);
           }
           
           suspendedTransaction = txManager.suspend();
           // set the tx in the invocation context to null now! - JBCACHE-785
           ctx.setTransaction(null);
           ctx.setGlobalTransaction(null);
            resumeSuspended = true;
        } else {
            suspendedTransaction = null;
            resumeSuspended = false;
        }

        Object result = null;

        try
        {
            // first of all deal with tx methods - these are only going to be
            // prepare/commit/rollback called by a remote cache, since calling
            // such methods on TreeCache directly would fail.

            if (isTransactionLifecycleMethod(m))
            {
                // this is a prepare, commit, or rollback.
                // start by setting transactional details into InvocationContext.
                ctx.setGlobalTransaction( findGlobalTransaction(m.getArgs()) );

                if (log.isDebugEnabled()) log.debug("Got gtx from method call " + ctx.getGlobalTransaction());
                ctx.getGlobalTransaction().setRemote( isRemoteGlobalTx(ctx.getGlobalTransaction()) );

                //replaceGtx(m, gtxFromMethodCall);
                if (ctx.getGlobalTransaction().isRemote()) remoteTransactions.put(ctx.getGlobalTransaction(),NULL);

                switch (m.getMethodId())
                {
                   case MethodDeclarations.optimisticPrepareMethod_id:
                   case MethodDeclarations.prepareMethod_id:
                      if (ctx.getGlobalTransaction().isRemote())
                      {
                          result = handleRemotePrepare(m, ctx.getGlobalTransaction());
                          scrubTxsOnExit = true;
                          if (cache.getUseInterceptorMbeans()&& statsEnabled)
                              m_prepares++;
                      }
                      else
                      {
                          if(log.isTraceEnabled()) log.trace("received my own message (discarding it)");
                          result = null;
                      }
                      break;
                   case MethodDeclarations.commitMethod_id:
                   case MethodDeclarations.rollbackMethod_id:
                      if (ctx.getGlobalTransaction().isRemote())
                      {
                          result = handleRemoteCommitRollback(m, ctx.getGlobalTransaction());
                          scrubTxsOnExit = true;
                      }
                      else
                      {
                          if (log.isTraceEnabled()) log.trace("received my own message (discarding it)");
                          result = null;
                      }
                      break;
                }
            }
            else
            {
                // non-transaction lifecycle method.
                result = handleNonTxMethod(m);
            }
        }
        catch (Exception e)
        {
            if (optionOverride == null || !optionOverride.isFailSilently()) throw e;
            log.trace("There was a problem handling this request, but " +
                  "failSilently was set, so suppressing exception", e);        
        }
        finally
        {
            if (resumeSuspended)
            {
                txManager.resume(suspendedTransaction);
            }
            else
            {
                if (ctx.getTransaction() != null && isValid(ctx.getTransaction()))
                {
                    copyInvocationScopeOptionsToTxScope(ctx);
                }
            }

            // we should scrub txs after every call to prevent race conditions
            // basically any other call coming in on the same thread and hijacking any running tx's
            // was highlighted in JBCACHE-606
            scrubInvocationCtx(scrubTxsOnExit);

        }
        return result;
    }

   private void setTransactionInContext(InvocationContext ctx, TransactionManager txManager)
   {
      Transaction tx = null;
      try
      {
         tx = txManager == null ? null : txManager.getTransaction();
      }
      catch (SystemException se)
      {
      }

      ctx.setTransaction( tx );//tx == null || !isValid(tx) ? null : tx); // make sure the tx is valid, otherwise set as null - see JBCACHE-785
      if (ctx.getTransaction() == null) ctx.setGlobalTransaction(null);  // nullify gtx as well
   }

   public long getPrepares()
    {
        return m_prepares;
    }

    public long getCommits()
    {
        return m_commits;
    }

    public long getRollbacks()
    {
        return m_rollbacks;
    }

    public void resetStatistics()
    {
        m_prepares = 0;
        m_commits = 0;
        m_rollbacks = 0;
    }

    public Map dumpStatistics()
    {
        Map retval=new HashMap(3);
        retval.put("Prepares", new Long(m_prepares));
        retval.put("Commits", new Long(m_commits));
        retval.put("Rollbacks", new Long(m_rollbacks));
        return retval;
    }

    protected GlobalTransaction findGlobalTransaction(Object[] params)
    {
        int clue = 0;

        if (params[clue] instanceof GlobalTransaction)
            return (GlobalTransaction) params[clue];
        else
            for (int i = 0; i < params.length; i++)
                if (params[i] instanceof GlobalTransaction) return (GlobalTransaction) params[i];
        return null;
    }


    private void copyInvocationScopeOptionsToTxScope(InvocationContext ctx)
    {
        // notify the transaction entry that this override is in place.
        TransactionEntry entry = txTable.get(ctx.getGlobalTransaction());
        if (entry != null)
        {
            Option txScopeOption = new Option();
            txScopeOption.setCacheModeLocal(ctx.getOptionOverrides() != null && ctx.getOptionOverrides().isCacheModeLocal());
            entry.setOption(txScopeOption);
        }
    }

    private Object handleRemotePrepare(JBCMethodCall m, GlobalTransaction gtx) throws Throwable
    {
        List modifications = (List) m.getArgs()[1];
        boolean onePhase = ((Boolean) m.getArgs()[cache.isNodeLockingOptimistic() ? 4 : 3]).booleanValue();

        // Is there a local transaction associated with GTX ?
        Transaction ltx = txTable.getLocalTransaction(gtx);

        Transaction currentTx = txManager.getTransaction();
        Object retval = null;

        try
        {
            if (ltx == null)
            {
                if (currentTx != null) txManager.suspend();
                ltx = createLocalTxForGlobalTx(gtx); // creates new LTX and associates it with a GTX
                if (log.isDebugEnabled())
                {
                    log.debug("(" + cache.getLocalAddress() + "): started new local TX as result of remote PREPARE: local TX=" + ltx + ", global TX=" + gtx);
                }
            }
            else
            {
                //this should be valid
                if (!isValid(ltx)) throw new CacheException("Transaction " + ltx + " not in correct state to be prepared");

                //associate this thread with this ltx if this ltx is NOT the current tx.
                if (currentTx == null || !ltx.equals(currentTx))
                {
                    txManager.suspend();
                    txManager.resume(ltx);
                }
            }


            if (log.isTraceEnabled()) {log.trace("Resuming existing transaction " + ltx + ", global TX=" + gtx);}

            // at this point we have a non-null ltx

            // Asssociate the local TX with the global TX. Create new
            // entry for TX in txTable, the modifications
            // below will need this entry to add their modifications
            // under the GlobalTx key
            if (txTable.get(gtx) == null)
            {
                // create a new transaction entry

                TransactionEntry entry = cache.isNodeLockingOptimistic() ? new OptimisticTransactionEntry() : new TransactionEntry();
                entry.setTransaction(ltx);
                log.debug("creating new tx entry");
                txTable.put(gtx, entry);
                if (log.isTraceEnabled()) log.trace("TxTable contents: " + txTable);
            }

            // register a sync handler for this tx.
            registerHandler(ltx, new RemoteSynchronizationHandler(gtx, ltx, cache));

            if (cache.isNodeLockingOptimistic())
                retval =  handleOptimisticPrepare(m, gtx, modifications, onePhase, ltx);
            else
                retval =  handlePessimisticPrepare(m, gtx, modifications, onePhase, ltx);
        }
        finally
        {
            txManager.suspend(); // suspends ltx - could be null
            // resume whatever else we had going.
            if (currentTx != null) txManager.resume(currentTx);
            if (log.isDebugEnabled()) log.debug("Finished remote prepare " + gtx);
        }

        return retval;
    }

    // --------------------------------------------------------------
    //   handler methods.
    // --------------------------------------------------------------

    /**
     * Tests if we already have a tx running.  If so, register a sync handler for this method invocation.
     * if not, create a local tx if we're using opt locking.
     * @param m
     * @return
     * @throws Throwable
     */
    private Object handleNonTxMethod(MethodCall m) throws Throwable
    {
        InvocationContext ctx = getInvocationContext();
        Transaction tx = ctx.getTransaction();
        Object result;
        // if there is no current tx and we're using opt locking, we need to use an implicit tx.
        boolean implicitTransaction = cache.isNodeLockingOptimistic() && tx == null;
        if (implicitTransaction)
        {
            tx = createLocalTx();
            // we need to attach this tx to the InvocationContext.
            ctx.setTransaction( tx );
        }
        if (tx != null) m = attachGlobalTransaction(tx, m);

        try
        {
            result = super.invoke(m);
            if (implicitTransaction)
            {
                copyInvocationScopeOptionsToTxScope(ctx);
                txManager.commit();
            }
        }
        catch (Throwable t)
        {
            if (implicitTransaction)
            {
                log.warn("Rolling back, exception encountered", t);
                result = t;
                try
                {
                    txManager.rollback();
                }
                catch (Throwable th)
                {
                    log.warn("Roll back failed encountered", th);
                }
            }
            else
            {
                throw t;
            }
        }
        return result;
    }

    private MethodCall attachGlobalTransaction(Transaction tx, MethodCall m) throws Exception
    {
        if (log.isDebugEnabled()) log.debug(" local transaction exists - registering global tx if not present for " + Thread.currentThread());
        if (log.isTraceEnabled())
        {
            GlobalTransaction tempGtx = txTable.get(tx);
            log.trace("Associated gtx in txTable is " + tempGtx);
        }

        // register a sync handler for this tx - only if the gtx is not remotely initiated.
        GlobalTransaction gtx = registerTransaction(tx);
        if (gtx != null)
        {
            m = replaceGtx(m, gtx);
        }
        else
        {
            // get the current gtx from the txTable.
            gtx = txTable.get(tx);
        }

        // make sure we attach this gtx to the invocation context.
        getInvocationContext().setGlobalTransaction(gtx);

        return m;
    }

    /**
     * This is called by invoke() if we are in a remote gtx's prepare() phase.
     * Finds the appropriate tx, suspends any existing txs, registers a sync handler
     * and passes up the chain.
     *
     * Resumes any existing txs before returning.
     * @param m
     * @return
     * @throws Throwable
     */
    private Object handleOptimisticPrepare(MethodCall m, GlobalTransaction gtx, List modifications, boolean onePhase, Transaction ltx) throws Throwable
    {
        // TODO: Manik: Refactor this to pass across entire workspace?
        Object retval;
        if (log.isDebugEnabled()) log.debug("Handling optimistic remote prepare " + gtx);
        replayModifications(modifications, ltx, true);
        retval = super.invoke(m);
        // JBCACHE-361 Confirm that the transaction is ACTIVE
        if (!isActive(ltx))
        {
            throw new ReplicationException("prepare() failed -- " +
                    "local transaction status is not STATUS_ACTIVE;" +
                    " is " + ltx.getStatus());
        }
        return retval;
    }

    private Object handlePessimisticPrepare(JBCMethodCall m, GlobalTransaction gtx, List modifications, boolean commit, Transaction ltx) throws Exception
    {
        boolean success = true;
        Object retval;
        try
        {
            // now pass up the prepare method itself.
            try
            {
                replayModifications(modifications, ltx, false);
                if (isOnePhaseCommitPrepareMehod(m))
                {
                    log.trace("Using one-phase prepare.  Not propagating the prepare call up the stack until called to do so by the sync handler.");
                }
                else
                {
                    super.invoke(m);
                }

                // JBCACHE-361 Confirm that the transaction is ACTIVE
                if (!isActive(ltx)) {
                   throw new ReplicationException("prepare() failed -- " +
                         "local transaction status is not STATUS_ACTIVE;" +
                         " is " + ltx.getStatus());
                }
            }
            catch (Throwable th)
            {
                log.error("prepare method invocation failed", th);
                retval = th;
                success = false;
                if (retval instanceof Exception)
                {
                    throw (Exception) retval;
                }
            }
        }
        finally
        {

            if (log.isTraceEnabled()) {log.trace("Are we running a 1-phase commit? " + commit);}
            // 4. If commit == true (one-phase-commit): commit (or rollback) the TX; this will cause
            //    {before/after}Completion() to be called in all registered interceptors: the TransactionInterceptor
            //    will then commit/rollback against the cache

            if (commit)
            {
                try
                {
//                    invokeOnePhaseCommitMethod(gtx, modifications.size() > 0, success);
                    if (success) ltx.commit() ; else ltx.rollback();
                }
                catch (Throwable t)
                {
                    log.error("Commit/rollback failed.", t);
                    if (success)
                    {
                        // try another rollback...
                        try
                        {
                            log.info("Attempting anotehr rollback");
                            //invokeOnePhaseCommitMethod(gtx, modifications.size() > 0, false);
                            ltx.rollback();
                        }
                        catch (Throwable t2)
                        {
                            log.error("Unable to rollback", t2);
                        }
                    }
                }
                finally
                {
                    transactions.remove(ltx);        // JBAS-298
                    remoteTransactions.remove(gtx); // JBAS-308
                }
            }
        }
        return null;
    }

    private Object replayModifications(List modifications, Transaction tx, boolean injectDataVersions)
    {
        Object retval = null;

        if (modifications != null)
        {
            for (Iterator it = modifications.iterator(); it.hasNext();)
            {
                JBCMethodCall method_call = (JBCMethodCall) it.next();
                try
                {
                   if (injectDataVersions && !MethodDeclarations.isDataGravitationMethod(method_call.getMethodId()))
                   {
                      Object[] origArgs = method_call.getArgs();
                      // there may be instances (e.g., data gravitation calls) where a data version is not passed in or not even relevant.
                      // make sure we are aware of this.
                      injectDataVersion(origArgs[origArgs.length - 1]);
                      // modify the call to the non-dataversioned counterpart since we've popped out the data version
                      Object[] args = new Object[origArgs.length - 1];
                      System.arraycopy(origArgs, 0, args, 0, args.length);

                      retval = super.invoke(MethodCallFactory.create(MethodDeclarations.getUnversionedMethod(method_call.getMethodId()), args));
                   }
                   else
                   {
                      retval = super.invoke(method_call);
                   }
                    if (!isActive(tx))
                    {
                        throw new ReplicationException("prepare() failed -- " + "local transaction status is not STATUS_ACTIVE; is " + tx.getStatus());
                    }
                }
                catch (Throwable t)
                {
                    log.error("method invocation failed", t);
                    retval = t;
                }
               finally
                {
                   // reset any options
                   if (injectDataVersions) getInvocationContext().setOptionOverrides(null);
                }

                if (retval != null && retval instanceof Exception)
                {
                    if (retval instanceof RuntimeException)
                        throw (RuntimeException) retval;
                    else
                        throw new RuntimeException((Exception) retval);
                }
            }
        }
        // need to pass up the prepare as well and return value from that
        return retval;
    }



   public void injectDataVersion(Object obj)
   {
      if (obj instanceof DataVersion)
      {
         Option o = new Option();
         o.setDataVersion((DataVersion) obj);
         getInvocationContext().setOptionOverrides(o);
      }
      else
      {
         log.debug("Object " + obj + " is not a DataVersion, not applying to this mod.");
      }
   }
    /**
     * Handles a commit or a rollback for a remote gtx.  Called by invoke().
     * @param m
     * @return
     * @throws Throwable
     */
    private Object handleRemoteCommitRollback(JBCMethodCall m, GlobalTransaction gtx) throws Throwable
    {
        Transaction ltx = null;
        try
        {
            ltx = getLocalTxForGlobalTx(gtx);
        }
        catch (IllegalStateException e)
        {
            if (m.getMethodId() == MethodDeclarations.rollbackMethod_id)
            {
                log.warn("No local transaction for this remotely originating rollback.  Possibly rolling back before a prepare call was broadcast?");
                return null;
            }
            else
            {
                throw e;
            }
        }

        // disconnect if we have a current tx associated
        Transaction currentTx = txManager.getTransaction();
        boolean resumeCurrentTxOnCompletion = false;
        try
        {
            if (!ltx.equals(currentTx))
            {
                currentTx = txManager.suspend();
                resumeCurrentTxOnCompletion = true;
                txManager.resume(ltx);
                // make sure we set this in the ctx
                getInvocationContext().setTransaction( ltx );
            }
            if (log.isDebugEnabled()) log.debug(" executing " + m + "() with local TX " + ltx + " under global tx " + gtx);

            // pass commit up the chain
            // super.invoke(m);
            // commit or rollback the tx.
            if (m.getMethodId() == MethodDeclarations.commitMethod_id)
            {
               txManager.commit();
               if (cache.getUseInterceptorMbeans()&& statsEnabled)
                  m_commits++;
            }
            else
            {
               txManager.rollback();
               if (cache.getUseInterceptorMbeans()&& statsEnabled)
                  m_rollbacks++;
            }
        }
        finally
        {
            //resume the old transaction if we suspended it
            if (resumeCurrentTxOnCompletion)
            {
                if (log.isTraceEnabled()) log.trace("Resuming suspended transaction " + currentTx);
                txManager.suspend();
                if (currentTx != null)
                {
                    txManager.resume(currentTx);
                    getInvocationContext().setTransaction( currentTx );
                }
            }

            // remove from local lists.
            remoteTransactions.remove(gtx);
            transactions.remove(ltx);

            // this tx has completed.  Clean up in the tx table.
            txTable.remove(gtx);
            txTable.remove(ltx);
        }

        if (log.isDebugEnabled()) log.debug("Finished remote commit/rollback method for " + gtx);

        return null;
    }

    private Transaction getLocalTxForGlobalTx(GlobalTransaction gtx) throws IllegalStateException
    {
        Transaction ltx = txTable.getLocalTransaction(gtx);
        if (ltx != null)
        {
            if (log.isDebugEnabled()) log.debug("Found local TX=" + ltx + ", global TX=" + gtx);
        }
        else
        {
            throw new IllegalStateException(" found no local TX for global TX " + gtx);
        }
        return ltx;
    }

    /**
     * Handles a commit or a rollback.  Called by the synch handler.  Simply tests that we are in the correct tx and
     * passes the meth call up the interceptor chain.
     * @param m
     * @return
     * @throws Throwable
     */
    private Object handleCommitRollback(MethodCall m) throws Throwable
    {
        GlobalTransaction gtx = findGlobalTransaction( m.getArgs() );
        Object result;

        // this must have a local transaction associated if a prepare has been
        // callled before
        //Transaction ltx = getLocalTxForGlobalTx(gtx);

//        Transaction currentTx = txManager.getTransaction();

        //if (!ltx.equals(currentTx)) throw new IllegalStateException(" local transaction " + ltx + " transaction does not match running tx " + currentTx);

        result = super.invoke(m);

        if (log.isDebugEnabled()) log.debug("Finished local commit/rollback method for " + gtx);
        return result;
    }

    // --------------------------------------------------------------
    //   Transaction phase runners
    // --------------------------------------------------------------

    /**
     * creates a commit() MethodCall and feeds it to handleCommitRollback();
     * @param gtx
     */
    protected void runCommitPhase(GlobalTransaction gtx, Transaction tx, List modifications, boolean onePhaseCommit)
    {
        // set the hasMods flag in the invocation ctx.  This should not be replicated, just used locally by the interceptors.
        getInvocationContext().setTxHasMods( modifications != null && modifications.size() > 0 );
        try
        {
            MethodCall commitMethod;
            if (onePhaseCommit)
            {
                // running a 1-phase commit.
                if (cache.isNodeLockingOptimistic())
                {
                    commitMethod = MethodCallFactory.create(MethodDeclarations.optimisticPrepareMethod, new Object[]{
                            gtx, modifications, null, (Address) cache.getLocalAddress(), Boolean.TRUE});
                }
                else
                {
                    commitMethod = MethodCallFactory.create(MethodDeclarations.prepareMethod, new Object[]
                            {gtx, modifications, (Address) cache.getLocalAddress(),
                                    Boolean.TRUE});
                }
            }
            else
            {
                commitMethod = MethodCallFactory.create(MethodDeclarations.commitMethod, new Object[]{gtx});
            }

            if (log.isTraceEnabled()) {log.trace(" running commit for " + gtx);}
            handleCommitRollback(commitMethod);
        }
        catch (Throwable e)
        {
            log.warn("Commit failed.  Clearing stale locks.");
            try
            {
                cleanupStaleLocks(gtx);
            }
            catch (RuntimeException re)
            {
               log.error("Unable to clear stale locks", re);
               throw re;
            }
            catch (Throwable e2)
            {
                log.error("Unable to clear stale locks", e2);
                throw new RuntimeException(e2);
            }

            if (e instanceof RuntimeException)
               throw (RuntimeException) e;
            else
               throw new RuntimeException("Commit failed.", e);
        }
    }


    private void cleanupStaleLocks(GlobalTransaction gtx) throws Throwable
    {
        TransactionEntry entry = txTable.get(gtx);
        if (entry != null)
           entry.releaseAllLocksLIFO(gtx);
    }

    /**
     * creates a rollback() MethodCall and feeds it to handleCommitRollback();
     * @param gtx
     */
    protected void runRollbackPhase(GlobalTransaction gtx, Transaction tx, List modifications)
    {
        //Transaction ltx = null;
        try
        {
            getInvocationContext().setTxHasMods( modifications != null && modifications.size() > 0 );
            // JBCACHE-457
//            MethodCall rollbackMethod = MethodCall(TreeCache.rollbackMethod, new Object[]{gtx, hasMods ? Boolean.TRUE : Boolean.FALSE});
            MethodCall rollbackMethod = MethodCallFactory.create(MethodDeclarations.rollbackMethod, new Object[]{gtx});
            if (log.isTraceEnabled()) {log.trace(" running rollback for " + gtx);}

            //JBCACHE-359 Store a lookup for the gtx so a listener
            // callback can find it
            //ltx = getLocalTxForGlobalTx(gtx);
            rollbackTransactions.put(tx, gtx);

            handleCommitRollback(rollbackMethod);
        }
        catch (Throwable e)
        {
            log.warn("Rollback had a problem", e);
        }
        finally
        {
            if (tx != null) rollbackTransactions.remove(tx);
        }
    }

    /**
     * Handles a local prepare - invoked by the sync handler.  Tests if the current tx matches the gtx passed in to the
     * method call and passes the prepare() call up the chain.
     *
     * @return
     * @throws Throwable
     */
    protected Object runPreparePhase(GlobalTransaction gtx, List modifications) throws Throwable
    {
        // TODO: Manik: one phase commit for opt locking too if using repl-async?
        // build the method call
        MethodCall prepareMethod;
//        if (cache.getCacheModeInternal() != TreeCache.REPL_ASYNC)
//        {
            // running a 2-phase commit.
            if (cache.isNodeLockingOptimistic())
            {
                prepareMethod = MethodCallFactory.create(MethodDeclarations.optimisticPrepareMethod, new Object[]{
                        gtx, modifications, null, (Address) cache.getLocalAddress(), Boolean.FALSE});
            }
            else if(cache.getCacheModeInternal() != TreeCache.REPL_ASYNC)
            {
                prepareMethod = MethodCallFactory.create(MethodDeclarations.prepareMethod,
                        new Object[]{gtx, modifications, (Address) cache.getLocalAddress(),
                                Boolean.FALSE}); // don't commit or rollback - wait for call
            }
        //}
        else
        {
            // this is a REPL_ASYNC call - do 1-phase commit.  break!
            log.trace("This is a REPL_ASYNC call (1 phase commit) - do nothing for beforeCompletion()");
            return null;
        }

        // passes a prepare call up the local interceptor chain.  The replication interceptor
        // will do the broadcasting if needed.  This is so all requests (local/remote) are
        // treated the same
        Object result;

        // Is there a local transaction associated with GTX ?
        Transaction ltx = txTable.getLocalTransaction(gtx);

        //if ltx is not null and it is already running
        if (txManager.getTransaction() != null && ltx != null && txManager.getTransaction().equals(ltx))
        {
            result = super.invoke(prepareMethod);
        }
        else
        {
            throw new CacheException(" local transaction " + ltx + " does not exist or does not match expected transaction " + gtx + " - perhaps the transaction has timed out?");
        }
        return result;
    }

    // --------------------------------------------------------------
    //   Private helper methods
    // --------------------------------------------------------------

    /**
     * Tests if a global transaction originated from a different cache in the cluster
     * @param gtx
     * @return true if the gtx is remote, false if it originated locally.
     */
    private boolean isRemoteGlobalTx(GlobalTransaction gtx)
    {
        return gtx != null && (gtx.getAddress() != null) && (!gtx.getAddress().equals(cache.getLocalAddress()));
    }

    /**
     * Creates a gtx (if one doesnt exist), a sync handler, and registers the tx.
     * @param tx
     * @return
     * @throws Exception
     */
    private GlobalTransaction registerTransaction(Transaction tx) throws Exception
    {
        GlobalTransaction gtx;
        if (isValid(tx) && transactions.put(tx, NULL) == null)
        {
            gtx = cache.getCurrentTransaction(tx);
            if (gtx.isRemote())
            {
                // should be no need to register a handler since this a remotely initiated gtx
                if (log.isTraceEnabled()) {log.trace("is a remotely initiated gtx so no need to register a tx for it");}
            }
            else
            {
                if (log.isTraceEnabled()) {log.trace("Registering sync handler for tx " + tx + ", gtx " + gtx);}
                LocalSynchronizationHandler myHandler = new LocalSynchronizationHandler(gtx, tx, cache);
                registerHandler(tx, myHandler);
            }
        }
        else if ((gtx = (GlobalTransaction) rollbackTransactions.get(tx)) != null)
        {
            if (log.isDebugEnabled()) log.debug("Transaction " + tx + " is already registered and is rolling back.");
        }
        else
        {
            if (log.isDebugEnabled()) log.debug("Transaction " + tx + " is already registered.");

        }
        return gtx;
    }

    /**
     * Registers a sync hander against a tx.
     * @param tx
     * @param handler
     * @throws Exception
     */
    private void registerHandler(Transaction tx, RemoteSynchronizationHandler handler) throws Exception
    {
        OrderedSynchronizationHandler orderedHandler = OrderedSynchronizationHandler.getInstance(tx);

        if (log.isTraceEnabled()) log.trace("registering for TX completion: SynchronizationHandler(" + handler + ")");

        orderedHandler.registerAtHead(handler); // needs to be invoked first on TX commit
    }

    /**
     * Replaces the global transaction in a method call with a new global transaction passed in.
     */
    private MethodCall replaceGtx(MethodCall m, GlobalTransaction gtx)
    {
        Class[] argClasses = m.getMethod().getParameterTypes();
        Object[] args = m.getArgs();

        for (int i = 0; i < argClasses.length; i++)
        {
            if (argClasses[i].equals(GlobalTransaction.class))
            {
                if (!gtx.equals(args[i]))
                {
                    args[i] = gtx;
                    m.setArgs(args);
                }
                break;
            }
        }
        return m;
    }

    /**
     * Creates and starts a local tx
     * @return
     * @throws Exception
     */
    private Transaction createLocalTx() throws Exception
    {
        if (log.isTraceEnabled()) {log.trace("Creating transaction for thread " + Thread.currentThread());}
        Transaction localTx;
        if (txManager == null) throw new Exception("Failed to create local transaction; TransactionManager is null");
        txManager.begin();
        localTx = txManager.getTransaction();
        return localTx;
    }

    /**
     * Creates a new local transaction for a given global transaction.
     * @param gtx
     * @return
     * @throws Exception
     */
    private Transaction createLocalTxForGlobalTx(GlobalTransaction gtx) throws Exception
    {
        Transaction localTx = createLocalTx();
        txTable.put(localTx, gtx);
        // attach this to the context
        getInvocationContext().setTransaction(localTx);
        if (log.isTraceEnabled()) log.trace("Created new tx for gtx " + gtx);
        return localTx;
    }

    private void setInvocationContext(Transaction tx, GlobalTransaction gtx)
    {
        InvocationContext ctx = getInvocationContext();
        ctx.setTransaction( tx );
        ctx.setGlobalTransaction( gtx );
    }

    private void scrubInvocationCtx(boolean removeTxs)
    {
        if (removeTxs) setInvocationContext(null, null);

        // only scrub options; not tx and gtx
        getInvocationContext().setOptionOverrides(null);
    }

    // ------------------------------------------------------------------------
    // Synchronization classes
    // ------------------------------------------------------------------------

    // this controls the whole transaction
    class RemoteSynchronizationHandler implements Synchronization
    {
        Transaction tx = null;
        GlobalTransaction gtx = null;
        TreeCache cache = null;
        List modifications = null;
        TransactionEntry entry = null;


        RemoteSynchronizationHandler(GlobalTransaction gtx, Transaction tx, TreeCache cache)
        {
            this.gtx = gtx;
            this.tx = tx;
            this.cache = cache;
        }

        public void beforeCompletion()
        {
               if (log.isTraceEnabled()) log.trace("Running beforeCompletion on gtx " + gtx);
               // make sure we refresh the tx from the tx_table as well, as it may have timed out.
               this.tx = txTable.getLocalTransaction(gtx);

               if (tx == null)
               {
                  log.error("Transaction is null in the transaction table.  Perhaps it timed out?");
                  throw new IllegalStateException("Transaction is null in the transaction table.  Perhaps it timed out?");
               }

               entry = txTable.get(gtx);
               if (entry == null)
               {
                   log.error("Transaction has a null transaction entry - beforeCompletion() will fail.");
                   log.error("TxTable contents: " + txTable);
                   throw new IllegalStateException("cannot find transaction entry for " + gtx);
               }

               modifications = entry.getModifications();
           }

        // this should really not be done here -
        // it is supposed to be post commit not actually run the commit
        public void afterCompletion(int status)
        {
            try
            {
                setInvocationContext(tx, gtx);
                if (log.isTraceEnabled()) log.trace("calling aftercompletion for " + gtx);
                // set any transaction wide options as current for this thread.
                if ((entry = txTable.get(gtx)) != null)
                {
                    modifications = entry.getModifications();
                    getInvocationContext().setOptionOverrides(entry.getOption());
                }
                transactions.remove(tx);

                switch (status)
                {
                    case Status.STATUS_COMMITTED:


                        // if this is optimistic or sync repl
                        boolean onePhaseCommit = !cache.isNodeLockingOptimistic() && cache.getCacheModeInternal() == TreeCache.REPL_ASYNC;
                        if (log.isDebugEnabled()) log.debug("Running commit phase.  One phase? " + onePhaseCommit);
                        runCommitPhase(gtx, tx, modifications, onePhaseCommit);
                        log.debug("Finished commit phase");
                        break;
                    case Status.STATUS_UNKNOWN:
                       log.warn("Received JTA STATUS_UNKNOWN in afterCompletion()!  XA resources may not be in sync.  The app should manually clean up resources at this point.");
                    case Status.STATUS_MARKED_ROLLBACK:
                    case Status.STATUS_ROLLEDBACK:
                        log.debug("Running rollback phase");
                        runRollbackPhase(gtx, tx, modifications);
                        log.debug("Finished rollback phase");
                        break;

                    default:
                        throw new IllegalStateException("illegal status: " + status);
                }
            }
            finally
            {
                // clean up the tx table
                txTable.remove(gtx);
                txTable.remove(tx);
                scrubInvocationCtx(true);
            }
        }

        public String toString()
        {
            return "TxInterceptor.RemoteSynchronizationHandler(gtx=" + gtx + ", tx=" + getTxAsString() + ")";
        }
        
        protected String getTxAsString()
        {
           // JBCACHE-1114 -- don't call toString() on tx or it can lead to stack overflow
           if (tx == null)
              return null;
           
           return tx.getClass().getName() + "@" + System.identityHashCode(tx);
        }
    }

    class LocalSynchronizationHandler extends RemoteSynchronizationHandler
    {
        private boolean localRollbackOnly = true;

        LocalSynchronizationHandler(GlobalTransaction gtx, Transaction tx, TreeCache cache)
        {
            super(gtx, tx, cache);
        }

        public void beforeCompletion()
        {
            super.beforeCompletion();
            // fetch the modifications before the transaction is committed
            // (and thus removed from the txTable)
            setInvocationContext(tx, gtx);
            if (modifications.size() == 0)
            {
                if (log.isTraceEnabled()) log.trace("No modifications in this tx.  Skipping beforeCompletion()");
                return;
            }

            // set any transaction wide options as current for this thread.
            getInvocationContext().setOptionOverrides( entry.getOption() );

            try
            {
                switch (tx.getStatus())
                {
                    // if we are active or preparing then we can go ahead
                    case Status.STATUS_ACTIVE:
                    case Status.STATUS_PREPARING:
                        // run a prepare call.
                        Object result = runPreparePhase(gtx, modifications);

                        if (result instanceof Throwable)
                        {
                           if (log.isDebugEnabled()) log.debug("Transaction needs to be rolled back - the cache returned an instance of Throwable for this prepare call (tx=" + tx + " and gtx=" + gtx + ")", (Throwable) result);
                           tx.setRollbackOnly();
                           throw (Throwable) result;
                        }
                        break;
                    default:
                        throw new CacheException("transaction " + tx + " in status " + tx.getStatus() + " unable to start transaction");
                }
            }
            catch (Throwable t)
            {
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
                   throw new RuntimeException("", t);
            }
            finally
            {
                localRollbackOnly = false;
                scrubInvocationCtx(false);
            }
        }

        public void afterCompletion(int status)
        {
            getInvocationContext().setLocalRollbackOnly( localRollbackOnly );
            super.afterCompletion(status);
        }

        public String toString()
        {
            return "TxInterceptor.LocalSynchronizationHandler(gtx=" + gtx + ", tx=" + getTxAsString() + ")";
        }
    }
}
