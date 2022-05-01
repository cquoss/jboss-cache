package org.jboss.cache.interceptors;

import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.TreeCache;
import org.jboss.cache.config.Option;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import java.lang.reflect.Method;

/**
 * Takes care of replicating modifications to other nodes in a cluster. Also
 * listens for prepare(), commit() and rollback() messages which are received
 * 'side-ways' (see docs/design/Refactoring.txt).
 *
 * @author Bela Ban
 * @version $Id: ReplicationInterceptor.java 2897 2006-11-10 20:03:34Z msurtani $
 */
public class ReplicationInterceptor extends BaseRpcInterceptor
{

    public Object invoke(MethodCall call) throws Throwable
    {

        JBCMethodCall m = (JBCMethodCall) call;
        InvocationContext ctx = getInvocationContext();
        GlobalTransaction gtx = ctx.getGlobalTransaction();

       // bypass for buddy group org metod calls.
       if (isBuddyGroupOrganisationMethod(m)) return super.invoke(m);
       

        boolean isLocalCommitOrRollback = gtx != null && !gtx.isRemote() && (m.getMethodId() == MethodDeclarations.commitMethod_id || m.getMethodId() == MethodDeclarations.rollbackMethod_id);


        // pass up the chain if not a local commit or rollback (in which case replicate first)
        Object o = isLocalCommitOrRollback ? null : super.invoke(m);

        Option optionOverride = ctx.getOptionOverrides();

        if (optionOverride != null && optionOverride.isCacheModeLocal() && ctx.getTransaction() == null)
        {
            log.trace("skip replication");
            return isLocalCommitOrRollback ? super.invoke(m) : o;
        }

        Method method = m.getMethod();

        // could be potentially TRANSACTIONAL. If so, we register for transaction completion callbacks (if we
        // have not yet done so
        if (ctx.getTransaction() != null)
        {
            if (gtx != null && !gtx.isRemote())
            {
                // lets see what sort of method we've got.
               switch(m.getMethodId())
               {
                  case MethodDeclarations.commitMethod_id:
                     // REPL_ASYNC will result in only a prepare() method - 1 phase commit.
                     if (containsModifications(m)) replicateCall(m, cache.getSyncCommitPhase());
                     // now pass up the chain
                     o = super.invoke(m);
                     break;
                  case MethodDeclarations.prepareMethod_id:
                     if (containsModifications(m)) {
                        // this is a prepare method
                        runPreparePhase(m, gtx); 
                     }
                     break;
                  case MethodDeclarations.rollbackMethod_id:
                     // REPL_ASYNC will result in only a prepare() method - 1 phase commit.
                     if (containsModifications(m) && !ctx.isLocalRollbackOnly()) replicateCall(m, cache.getSyncRollbackPhase());
                     // now pass up the chain
                     o = super.invoke(m);
                     break;
               }
            }
        }
        else if (MethodDeclarations.isCrudMethod(method))
        {
            // NON-TRANSACTIONAL and CRUD method
            if (log.isTraceEnabled()) log.trace("Non-tx crud meth");
            if (ctx.isOriginLocal())
            {
                // don't re-broadcast if we've received this from anotehr cache in the cluster.
                handleReplicatedMethod(m, cache.getCacheModeInternal());
            }
        }
        else
        {
            if (log.isTraceEnabled()) log.trace("Non-tx and non crud meth");
        }

        return o;
    }

    void handleReplicatedMethod(JBCMethodCall m, int mode) throws Throwable
    {
        if (mode == TreeCache.REPL_SYNC && (m.getMethodId() == MethodDeclarations.putFailFastKeyValueMethodLocal_id))
        {
            if (log.isTraceEnabled())
            {
                log.trace("forcing asynchronous replication for putFailFast()");
            }
            mode = TreeCache.REPL_ASYNC;
        }
        if (log.isTraceEnabled())
        {
            log.trace("invoking method " + m + ", members=" + cache.getMembers() + ", mode=" +
                    cache.getCacheMode() + ", exclude_self=" + true + ", timeout=" +
                    cache.getSyncReplTimeout());
        }
        switch (mode)
        {
            case TreeCache.REPL_ASYNC:
                // 2. Replicate change to all *other* members (exclude self !)
                replicateCall(m, false);
                break;
            case TreeCache.REPL_SYNC:
                // REVISIT Needs to exclude itself and apply the local change manually.
                // This is needed such that transient field is modified properly in-VM.
                replicateCall(m, true);
                break;
        }
    }

    /**
     * Calls prepare(GlobalTransaction,List,org.jgroups.Address,boolean)) in all members except self.
     * Waits for all responses. If one of the members failed to prepare, its return value
     * will be an exception. If there is one exception we rethrow it. This will mark the
     * current transaction as rolled back, which will cause the
     * afterCompletion(int) callback to have a status
     * of <tt>MARKED_ROLLBACK</tt>. When we get that call, we simply roll back the
     * transaction.<br/>
     * If everything runs okay, the afterCompletion(int)
     * callback will trigger the @link #runCommitPhase(GlobalTransaction)).
     * <br/>
     *
     * @throws Exception
     */
    protected void runPreparePhase(JBCMethodCall prepareMethod, GlobalTransaction gtx) throws Throwable
    {
        boolean async = cache.getCacheModeInternal() == TreeCache.REPL_ASYNC;
        if (log.isTraceEnabled())
        {
            log.trace("(" + cache.getLocalAddress() + "): running remote prepare for global tx " + gtx + " with async mode=" + async);
        }

        // this method will return immediately if we're the only member (because exclude_self=true)
        replicateCall(prepareMethod, !async);
    }
}
