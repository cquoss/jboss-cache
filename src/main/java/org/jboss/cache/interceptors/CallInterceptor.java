package org.jboss.cache.interceptors;

import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.TreeCache;
import org.jboss.cache.config.Option;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Transaction;
import java.util.HashSet;
import java.util.Set;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection.
 * If the call resulted in a modification, add the Modification to the end of the modification list
 * keyed by the current transaction.
 *
 * Although always added to the end of an optimistically locked chain as well, calls should not make it down to
 * this interceptor unless it is a call the OptimisticNodeInterceptor knows nothing about.
 *
 * @author Bela Ban
 * @version $Id: CallInterceptor.java 2061 2006-06-12 20:24:32Z msurtani $
 */
public class CallInterceptor extends Interceptor
{
    private static Set transactionLifecycleMethods = new HashSet();
    static
    {
        transactionLifecycleMethods.add(MethodDeclarations.commitMethod);
        transactionLifecycleMethods.add(MethodDeclarations.rollbackMethod);
        transactionLifecycleMethods.add(MethodDeclarations.prepareMethod);
        transactionLifecycleMethods.add(MethodDeclarations.optimisticPrepareMethod);
    }


    public void setCache(TreeCache cache)
    {
        super.setCache(cache);
    }

    public Object invoke(MethodCall m) throws Throwable
    {

        Object retval = null;

        if (!transactionLifecycleMethods.contains(m.getMethod()))
        {
            if (log.isTraceEnabled()) log.trace("Invoking method " + m + " on cache.");
            try
            {
                retval = m.invoke(cache);
            }
            catch (Throwable t)
            {
                retval = t;
            }
        }
        else
        {
            if (log.isTraceEnabled()) log.trace("Suppressing invocation of method " + m + " on cache.");
        }

        InvocationContext ctx = getInvocationContext();
        Transaction tx = ctx.getTransaction();
        if (tx != null && isValid(tx))
        {
            // test for exceptions.
            if (retval instanceof Throwable)
            {
                tx.setRollbackOnly(); // no exception, regular return
            }
            else
            {
                // only add the modification to the modification list if we are using pessimistic locking.
                // Optimistic locking calls *should* not make it this far down the interceptor chain, but just
                // in case a method has been invoked that the OptimisticNodeInterceptor knows nothing about, it will
                // filter down here.

                if (!cache.isNodeLockingOptimistic() && MethodDeclarations.isCrudMethod(m.getMethod()))
                {
                    // if method is a CRUD (Create/Remove/Update/Delete) method: add it to the modification
                    // list, otherwise skip (e.g. get() is not added)
                    // add the modification to the TX's modification list. this is used to later
                    // (on TX commit) send all modifications done in this TX to all members
                    GlobalTransaction gtx = ctx.getGlobalTransaction();
                    if (gtx == null)
                    {
                        if (log.isDebugEnabled())
                        {
                            log.debug("didn't find GlobalTransaction for " + tx + "; won't add modification to transaction list");
                        }
                    }
                    else
                    {
                        Option o = getInvocationContext().getOptionOverrides();
                        if (o != null && o.isCacheModeLocal())
                        {
                            log.debug("Not adding method to modification list since cache mode local is set.");
                        }
                        else
                        {
                            cache.getTransactionTable().addModification(gtx, m);
                        }
                        if (cache.getCacheLoaderManager() != null) cache.getTransactionTable().addCacheLoaderModification(gtx, m);
                    }
                }
            }
        }

        if (retval instanceof Throwable)
        {
            throw (Throwable) retval;
        }

        return retval;
    }
}
