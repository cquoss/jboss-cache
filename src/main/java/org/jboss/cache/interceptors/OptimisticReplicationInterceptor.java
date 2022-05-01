/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.interceptors;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.OptimisticTransactionEntry;
import org.jboss.cache.TreeCache;
import org.jboss.cache.config.Option;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.optimistic.DataVersion;
import org.jboss.cache.optimistic.DefaultDataVersion;
import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jboss.cache.optimistic.WorkspaceNode;
import org.jgroups.blocks.MethodCall;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Replication interceptor for the optimistically locked interceptor chain
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:stevew@jofti.com">Steve Woodcock (stevew@jofti.com)</a>
 */
public class OptimisticReplicationInterceptor extends BaseRpcInterceptor
{

    //record of local broacasts - so we do not broadcast rollbacks/commits that resuted from
    // local prepare failures
    private Map broadcastTxs = new ConcurrentHashMap();


    public void setCache(TreeCache cache)
    {
        super.setCache(cache);
    }

    public Object invoke(MethodCall call) throws Throwable
    {
        JBCMethodCall m = (JBCMethodCall) call;
        InvocationContext ctx = getInvocationContext();
        Option optionOverride = ctx.getOptionOverrides();
       // bypass for buddy group org metod calls.
       if (isBuddyGroupOrganisationMethod(m)) return super.invoke(m);
       
        if (optionOverride != null && optionOverride.isCacheModeLocal() && ctx.getTransaction() == null)
        {
            // skip replication!!
            return super.invoke(m);
        }
        
        Object retval;

        //we need a transaction to be present in order to do this
        if (ctx.getTransaction() != null)
        {

            // get the current gtx
            GlobalTransaction gtx = ctx.getGlobalTransaction();
            if (gtx == null)
            {
                throw new CacheException("failed to get global transaction");
            }
            log.debug(" received method " + m);

            // on a  local prepare we first run the prepare -
            //if this works broadcast it

            switch (m.getMethodId())
            {
               case MethodDeclarations.optimisticPrepareMethod_id:
                  // pass up the chain.
                  retval = super.invoke(m);

                  if (!gtx.isRemote() && getInvocationContext().isOriginLocal())
                  {
                      // replicate the prepare call.
                      retval = broadcastPrepare(m, gtx);
                      //if we have an exception then the remote methods failed
                      if (retval instanceof Throwable)
                      {
                          throw (Throwable) retval;
                      }
                  }
                  break;
               case MethodDeclarations.commitMethod_id:
                  //lets broadcast the commit first
                  Throwable temp = null;
                  if (!gtx.isRemote() && getInvocationContext().isOriginLocal() && broadcastTxs.containsKey(gtx))
                  {
                      //we dont do anything
                      try
                      {
                          broadcastCommit(gtx);
                      }
                      catch (Throwable t)
                      {
                          log.error(" a problem occurred with remote commit", t);
                          temp = t;
                      }
                  }

                  retval = super.invoke(m);
                  if (temp != null)
                  {
                      throw temp;
                  }
                  break;
               case MethodDeclarations.rollbackMethod_id:
                  //    lets broadcast the rollback first
                  Throwable temp2 = null;
                  if (!gtx.isRemote() && getInvocationContext().isOriginLocal() && broadcastTxs.containsKey(gtx))
                  {
                      //we dont do anything
                      try
                      {
                          broadcastRollback(gtx);
                      }
                      catch (Throwable t)
                      {
                          log.error(" a problem occurred with remote rollback", t);
                          temp2 = t;
                      }

                  }
                  retval = super.invoke(m);
                  if (temp2 != null)
                  {
                      throw temp2;
                  }
                  break;
               default :
                  //it is something we do not care about
                  log.debug(" received method " + m + " not handling");
                  retval = super.invoke(m);
                  break;
            }
        }
        else
        {
            throw new CacheException("transaction does not exist");
        }
        return retval;
    }

    protected Object broadcastPrepare(JBCMethodCall methodCall, GlobalTransaction gtx) throws Throwable
    {
        boolean remoteCallSync = cache.getCacheModeInternal() == TreeCache.REPL_SYNC;

        Object[] args = methodCall.getArgs();
        List modifications = (List) args[1];
        int num_mods = modifications != null ? modifications.size() : 0;

       // See JBCACHE-843 and docs/design/DataVersioning.txt
       JBCMethodCall toBroadcast = mapDataVersionedMethodCalls(methodCall, getTransactionWorkspace(gtx));

        // this method will return immediately if we're the only member (because
        // exclude_self=true)

        if (cache.getMembers() != null && cache.getMembers().size() > 1)
        {

            //record the things we have possibly sent
            broadcastTxs.put(gtx, gtx);
            if (log.isDebugEnabled()) log.debug("(" + cache.getLocalAddress()
                      + "): broadcasting prepare for " + gtx
                      + " (" + num_mods + " modifications");

            replicateCall(toBroadcast, remoteCallSync);
        }
        else
        {
            //no members, ignoring
            if (log.isDebugEnabled()) log.debug("(" + cache.getLocalAddress()
                      + "):not broadcasting prepare as members are " + cache.getMembers());
        }
        return null;
    }

   private JBCMethodCall mapDataVersionedMethodCalls(JBCMethodCall m, TransactionWorkspace w)
   {
      Object[] origArgs = m.getArgs();
      return MethodCallFactory.create(m.getMethod(), new Object[]{origArgs[0], translate((List) origArgs[1], w), origArgs[2], origArgs[3], origArgs[4] });
   }

   /**
    * Translates a list of MethodCalls from non-versioned calls to versioned calls.
    */
   private List translate(List l, TransactionWorkspace w)
   {
      List newList = new ArrayList();
      Iterator origCalls = l.iterator();
      while (origCalls.hasNext())
      {
         JBCMethodCall origCall = (JBCMethodCall) origCalls.next();
         if (MethodDeclarations.isDataGravitationMethod(origCall.getMethodId()))
         {
            // no need to translate data gravitation calls.
            newList.add(origCall);
         }
         else
         {
            Object[] origArgs = origCall.getArgs();
            // get the data version associated with this orig call.

            // since these are all crud methods the Fqn is at arg subscript 1.
            Fqn fqn = (Fqn) origArgs[1];
            // now get a hold of the data version for this specific modification
            DataVersion versionToBroadcast = getVersionToBroadcast(w, fqn);

            // build up the new arguments list for the new call.  Identical to the original lis except that it has the
            // data version tacked on to the end.
            Object[] newArgs = new Object[origArgs.length + 1];
            for (int i=0; i<origArgs.length; i++) newArgs[i] = origArgs[i];
            newArgs[origArgs.length] = versionToBroadcast;

            // now create a new method call which contains this data version
            JBCMethodCall newCall = MethodCallFactory.create(MethodDeclarations.getVersionedMethod(origCall.getMethodId()), newArgs);

            // and add it to the new list.
            newList.add(newCall);
         }
      }
      return newList;
   }

   /**
    * Digs out the DataVersion for a given Fqn.  If the versioning is explicit, it is passed as-is.  If implicit, it is
    * cloned and then incremented, and the clone is returned.
    */
   private DataVersion getVersionToBroadcast(TransactionWorkspace w, Fqn f)
   {
      WorkspaceNode n = w.getNode(f);
      if (n == null)
      {
         if (log.isTraceEnabled()) log.trace("Fqn " + f + " not found in workspace; not using a data version.");
         return null;
      }
      if (n.isVersioningImplicit())
      {
         DefaultDataVersion v = (DefaultDataVersion) n.getVersion();
         if (log.isTraceEnabled()) log.trace("Fqn " + f + " has implicit versioning.  Broadcasting an incremented version.");
         return v.increment();
      }
      else
      {
         if (log.isTraceEnabled()) log.trace("Fqn " + f + " has explicit versioning.  Broadcasting the version as-is.");
         return n.getVersion();
      }
   }


    protected void broadcastCommit(GlobalTransaction gtx) throws Throwable
    {
        boolean remoteCallSync = cache.getSyncCommitPhase();

        // 1. Multicast commit() to all members (exclude myself though)
        if (cache.getMembers() != null && cache.getMembers().size() > 1)
        {
            try
            {
                broadcastTxs.remove(gtx);
                JBCMethodCall commit_method = MethodCallFactory.create(MethodDeclarations.commitMethod,
                                                          new Object[]{gtx});

                log.debug("running remote commit for " + gtx
                          + " and coord=" + cache.getLocalAddress());

                replicateCall(commit_method, remoteCallSync);
            }
            catch (Exception e)
            {
                log.fatal("commit failed", e);
                throw e;
            }
        }
        else
        {
            // ignoring
        }
    }

   protected TransactionWorkspace getTransactionWorkspace(GlobalTransaction gtx) throws CacheException
   {
       OptimisticTransactionEntry transactionEntry = (OptimisticTransactionEntry) cache.getTransactionTable().get(gtx);

       if (transactionEntry == null)
       {
           throw new CacheException("unable to map global transaction " + gtx + " to transaction entry");
       }

       // try and get the workspace from the transaction
       return transactionEntry.getTransactionWorkSpace();
   }

    protected void broadcastRollback(GlobalTransaction gtx) throws Throwable
    {
        boolean remoteCallSync = cache.getSyncRollbackPhase();

        if (cache.getMembers() != null && cache.getMembers().size() > 1)
        {
            // 1. Multicast rollback() to all other members (excluding myself)
            try
            {
                broadcastTxs.remove(gtx);
                JBCMethodCall rollback_method = MethodCallFactory.create(MethodDeclarations.rollbackMethod, new Object[]{gtx});

                log.debug("running remote rollback for " + gtx
                          + " and coord=" + cache.getLocalAddress());
                replicateCall( rollback_method, remoteCallSync );

            }
            catch (Exception e)
            {
                log.error("rollback failed", e);
                throw e;
            }
        }
    }
}