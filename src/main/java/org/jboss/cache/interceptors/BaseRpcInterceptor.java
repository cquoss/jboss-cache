/**
 * 
 */
package org.jboss.cache.interceptors;

import org.jboss.cache.TreeCache;
import org.jboss.cache.buddyreplication.BuddyManager;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jgroups.blocks.MethodCall;

import java.util.Iterator;
import java.util.List;

/**
 * Acts as a base for all RPC calls - subclassed by {@see ReplicationInterceptor} and {@see OptimisticReplicationInterceptor}.
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public abstract class BaseRpcInterceptor extends Interceptor
{

    private BuddyManager buddyManager;
    private boolean usingBuddyReplication;

    public void setCache(TreeCache cache)
    {
       this.cache = cache;
       buddyManager = cache.getBuddyManager();
       usingBuddyReplication = buddyManager != null;
    }

//
//   public Object replicate(MethodCall method_call) throws Throwable {
//      try {
//         Interceptor.getInvocationContext().setOriginLocal(false);
//         return super.invoke(method_call);
//      }
//      finally {
//         Interceptor.getInvocationContext().setOriginLocal(true);
//      }
//   }
//
//   public void replicate(List method_calls) throws Throwable {
//      try {
//         Interceptor.getInvocationContext().setOriginLocal(false);
//         MethodCall method_call;
//         for(Iterator it=method_calls.iterator(); it.hasNext();) {
//            method_call=(MethodCall)it.next();
//            super.invoke(method_call);
//         }
//      }
//      finally {
//         Interceptor.getInvocationContext().setOriginLocal(true);
//      }
//   }
//

   /**
     * Checks whether any of the responses are exceptions. If yes, re-throws
     * them (as exceptions or runtime exceptions).
     *
     * @param rsps
     * @throws Throwable
     */
    protected void checkResponses(List rsps) throws Throwable
    {
        Object rsp;
        if (rsps != null)
        {
            for (Iterator it = rsps.iterator(); it.hasNext();)
            {
                rsp = it.next();
                if (rsp != null && rsp instanceof Throwable)
                {
                    // lets print a stack trace first.
                    if (log.isDebugEnabled())
                        log.debug("Received Throwable from remote node", (Throwable) rsp);
                    throw (Throwable) rsp;
                }
            }
        }
    }

    protected void replicateCall(JBCMethodCall call, boolean sync) throws Throwable
    {
        replicateCall(null, call, sync);
    }

    protected void replicateCall(List recipients, JBCMethodCall call, boolean sync) throws Throwable
    {

        if (log.isTraceEnabled()) log.trace("Broadcasting call " + call + " to recipient list " + recipients);

        if (!sync && cache.getUseReplQueue() && cache.getReplQueue() != null && !usingBuddyReplication)
        {
            putCallOnAsyncReplicationQueue( call );
        }
        else
        {
            if (usingBuddyReplication) call = buddyManager.transformFqns(call);

            List callRecipients = recipients;
            if (callRecipients == null)
            {
                callRecipients = usingBuddyReplication ? buddyManager.getBuddyAddresses() : cache.getMembers();
            }

            List rsps = cache.callRemoteMethods(callRecipients,
                                                MethodDeclarations.replicateMethod,
                                                new Object[]{call},
                                                sync, // is synchronised?
                                                true, // ignore self?
                                                cache.getSyncReplTimeout());
            if (log.isTraceEnabled())
            {
               log.trace("responses=" + rsps);
            }
            if (sync) checkResponses(rsps);
        }

    }

    protected void putCallOnAsyncReplicationQueue(MethodCall call)
    {
        // should this be:
        // cache.getReplQueue().add(call); // ??
        if (log.isDebugEnabled()) log.debug("Putting call " + call + " on the replication queue.");
        cache.getReplQueue().add(MethodCallFactory.create(MethodDeclarations.replicateMethod, new Object[]{call}));
    }

 /*  private void checkForNonSerializableArgs(MethodCall method_call) throws NotSerializableException {
      Object[] args=method_call.getArgs();
      if(args != null && args.length > 0) {
         for(int i=0; i < args.length; i++) {
            Object arg=args[i];
            if(arg != null) {
               if(!(arg instanceof Serializable || arg instanceof Externalizable))
                  throw new NotSerializableException(arg.getClass().getName());
            }
         }
      }
   }*/

    protected boolean containsModifications(JBCMethodCall m)
    {
        switch (m.getMethodId())
        {
           case MethodDeclarations.prepareMethod_id:
           case MethodDeclarations.optimisticPrepareMethod_id:
              List mods = (List)m.getArgs()[1];
              return mods.size() > 0;
           case MethodDeclarations.commitMethod_id:
           case MethodDeclarations.rollbackMethod_id:
              return getInvocationContext().isTxHasMods();
           default :
              return false;
        }
    }
}