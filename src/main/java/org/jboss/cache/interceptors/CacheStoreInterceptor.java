package org.jboss.cache.interceptors;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.Modification;
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TransactionTable;
import org.jboss.cache.TreeCache;
import org.jboss.cache.config.CacheLoaderConfig;
import org.jboss.cache.loader.CacheLoader;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jgroups.blocks.MethodCall;

import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Writes modifications back to the store on the way out: stores modifications back
 * through the CacheLoader, either after each method call (no TXs), or at TX commit.
 * @author Bela Ban
 * @version $Id: CacheStoreInterceptor.java 4069 2007-06-24 21:17:25Z jawilson $
 */
public class CacheStoreInterceptor extends Interceptor implements CacheStoreInterceptorMBean
{

   protected CacheLoaderConfig loaderConfig = null;
   protected TransactionManager tx_mgr=null;
   protected TransactionTable   tx_table=null;
   private HashMap m_txStores = new HashMap();
   private Map preparingTxs = new ConcurrentHashMap();
   private long m_cacheStores = 0;
   protected CacheLoader loader;

   public void setCache(TreeCache cache) {
      super.setCache(cache);
      this.loaderConfig = cache.getCacheLoaderManager().getCacheLoaderConfig();
      tx_mgr=cache.getTransactionManager();
      tx_table=cache.getTransactionTable();
      this.loader = cache.getCacheLoaderManager().getCacheLoader();
   }

   /**
    * Pass the method on. When it returns, store the modification back to the store using the CacheLoader.
    * In case of a transaction, register for TX completion (2PC) and at TX commit, write modifications made
    * under the given TX to the CacheLoader
    * @param call
    * @return
    * @throws Throwable
    */
   public Object invoke(MethodCall call) throws Throwable {

      JBCMethodCall m = (JBCMethodCall) call;

      // if this is a shared cache loader and the call is of remote origin, pass up the chain. - Manik
      // see http://www.jboss.com/index.html?module=bb&op=viewtopic&t=76090

      if (!getInvocationContext().isOriginLocal() && loaderConfig.isShared()) {
          log.trace("Passing up method call and bypassing this interceptor since the cache loader is shared and this call originated remotely.");
          return super.invoke(m);
      }

      Fqn          fqn;
      Map          attributes;
      Object       key, value;
      Method       meth=m.getMethod();
      Object[]     args=m.getArgs();
      Object       retval, tmp_retval=null;
      boolean      use_tmp_retval=false;


       if (log.isTraceEnabled()) {
           log.trace("CacheStoreInterceptor called with meth " + m);
       }

       if (tx_mgr != null && tx_mgr.getTransaction() != null) {
           // we have a tx running.
           log.trace("transactional so don't put stuff in the cloader yet.");
           GlobalTransaction gtx = getInvocationContext().getGlobalTransaction();
           switch (m.getMethodId())
           {
              case MethodDeclarations.commitMethod_id:
                 if (getInvocationContext().isTxHasMods()) {
                    // this is a commit call.
                    if (log.isTraceEnabled()) log.trace("Calling loader.commit() for gtx " + gtx);
                    // sync call (a write) on the loader
                    List fqnsModified = getFqnsFromModificationList(tx_table.get(gtx).getCacheLoaderModifications());
                    try
                    {
                         loader.commit(gtx);
                    }
                    finally
                    {
                        preparingTxs.remove(gtx);
                    }
                    if (cache.getUseInterceptorMbeans()&& statsEnabled) {
                       Integer puts = (Integer)m_txStores.get(gtx);
                       if (puts != null)
                          m_cacheStores = m_cacheStores + puts.intValue();
                       m_txStores.remove(gtx);
                    }
                 }
                 else {
                    log.trace("Commit called with no modifications; ignoring.");
                 }
                 break;
              case MethodDeclarations.rollbackMethod_id:
                 if (getInvocationContext().isTxHasMods()) {
                    // this is a rollback method
                    if (preparingTxs.containsKey(gtx))
                    {
                        preparingTxs.remove(gtx);
                        loader.rollback(gtx);
                    }
                    if (cache.getUseInterceptorMbeans()&& statsEnabled)
                       m_txStores.remove(gtx);
                 }
                 else {
                    log.trace("Rollback called with no modifications; ignoring.");
                 }
                 break;
              case MethodDeclarations.optimisticPrepareMethod_id:
              case MethodDeclarations.prepareMethod_id:
                 prepareCacheLoader(gtx, isOnePhaseCommitPrepareMehod(m));
                 break;
           }

           // pass up the chain
           return super.invoke(m);
       }

      // if we're here we don't run in a transaction

      // remove() methods need to be applied to the CacheLoader before passing up the call: a listener might
      // access an element just removed, causing the CacheLoader to *load* the element before *removing* it.
//      synchronized(this) {
       switch (m.getMethodId())
       {
          case MethodDeclarations.removeNodeMethodLocal_id:
             fqn=(Fqn)args[1];
             loader.remove(fqn);
             break;
          case MethodDeclarations.removeKeyMethodLocal_id:
             fqn=(Fqn)args[1];
             key=args[2];
             tmp_retval=loader.remove(fqn, key);
             use_tmp_retval=true;
             break;
          case MethodDeclarations.removeDataMethodLocal_id:
             fqn=(Fqn)args[1];
             loader.removeData(fqn);
             break;
       }
//      }

      retval=super.invoke(m);

      // put() methods need to be applied *after* the call
//      synchronized(this) {
      switch (m.getMethodId())
      {
         case MethodDeclarations.putDataMethodLocal_id:
         case MethodDeclarations.putDataEraseMethodLocal_id:
            Modification mod = convertMethodCallToModification(m);
            log.debug(mod);
            fqn = mod.getFqn();

            loader.put(Collections.singletonList(mod));
            if (cache.getUseInterceptorMbeans()&& statsEnabled)
               m_cacheStores++;
            break;
         case MethodDeclarations.putKeyValMethodLocal_id:
            fqn=(Fqn)args[1];
            key=args[2];
            value=args[3];
            tmp_retval = loader.put(fqn, key, value);
            use_tmp_retval = true;
            if (cache.getUseInterceptorMbeans()&& statsEnabled)
               m_cacheStores++;
            break;
      }
//      }

      if(use_tmp_retval)
         return tmp_retval;
      else
         return retval;
   }

    private List getFqnsFromModificationList(List modifications)
    {
        Iterator it = modifications.iterator();
        List fqnList = new ArrayList();
        while (it.hasNext())
        {
            MethodCall mc = (MethodCall) it.next();
            Fqn fqn = findFqn(mc.getArgs());
            if (fqn != null && !fqnList.contains(fqn)) fqnList.add(fqn);
        }
        return fqnList;
    }

    private Fqn findFqn(Object[] o)
    {
        for (int i=0; i<o.length; i++)
        {
            if (o[i] instanceof Fqn) return (Fqn) o[i];
        }
        return null;
    }

    public long getCacheLoaderStores() {
       return m_cacheStores;
    }

   public void resetStatistics() {
      m_cacheStores = 0;
   }

   public Map dumpStatistics() {
      Map retval=new HashMap();
      retval.put("CacheLoaderStores", new Long(m_cacheStores));
      return retval;
   }

   private void prepareCacheLoader(GlobalTransaction gtx, boolean onePhase) throws Exception {
      List modifications;
      TransactionEntry entry;
      int txPuts = 0;

      entry=tx_table.get(gtx);
      if(entry == null)
         throw new Exception("entry for transaction " + gtx + " not found in transaction table");
      modifications=entry.getCacheLoaderModifications();
      if(modifications.size() == 0)
         return;
      List cache_loader_modifications=new ArrayList();
      for(Iterator it=modifications.iterator(); it.hasNext();) {
         JBCMethodCall methodCall=(JBCMethodCall) it.next();
         Modification mod=convertMethodCallToModification(methodCall);
         cache_loader_modifications.add(mod);
         if (cache.getUseInterceptorMbeans()&& statsEnabled) {
            if ( (mod.getType() == Modification.PUT_DATA) ||
                 (mod.getType() == Modification.PUT_DATA_ERASE) ||
                 (mod.getType() == Modification.PUT_KEY_VALUE) )
               txPuts++;
         }
      }
      if (log.isTraceEnabled()) log.trace("Converted method calls to cache loader modifications.  List size: " + cache_loader_modifications.size());
      if(cache_loader_modifications.size() > 0) {
         loader.prepare(gtx, cache_loader_modifications, onePhase);
         preparingTxs.put(gtx, gtx);
         if (cache.getUseInterceptorMbeans()&& statsEnabled && txPuts > 0)
            m_txStores.put(gtx, new Integer(txPuts));
      }
   }

   protected Modification convertMethodCallToModification(JBCMethodCall methodCall) throws Exception {
      Method method=methodCall.getMethod();
      Object[] args;
      if(method == null)
         throw new Exception("method call has no method: " + methodCall);

      args=methodCall.getArgs();
      switch (methodCall.getMethodId())
      {
         case MethodDeclarations.putDataMethodLocal_id:
            return new Modification(Modification.PUT_DATA,
                  (Fqn)args[1],      // fqn
                  (Map)args[2]);     // data
         case MethodDeclarations.putDataEraseMethodLocal_id:
            return new Modification(Modification.PUT_DATA_ERASE,
                  (Fqn)args[1],      // fqn
                  (Map)args[2]);     // data
         case MethodDeclarations.putKeyValMethodLocal_id:
            return new Modification(Modification.PUT_KEY_VALUE,
                  (Fqn)args[1],      // fqn
                  args[2],           // key
                  args[3]);          // value
         case MethodDeclarations.removeNodeMethodLocal_id:
            return new Modification(Modification.REMOVE_NODE,
                  (Fqn)args[1]);     // fqn
         case MethodDeclarations.removeKeyMethodLocal_id:
            return new Modification(Modification.REMOVE_KEY_VALUE,
                  (Fqn)args[1],      // fqn
                  args[2]);          // key
         case MethodDeclarations.removeDataMethodLocal_id:
            return new Modification(Modification.REMOVE_DATA,
                  (Fqn)args[1]);     // fqn
         default :
            throw new Exception("method call " + method.getName() + " cannot be converted to a modification");
      }
   }
}
