package org.jboss.cache.interceptors;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.Modification;
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TransactionTable;
import org.jboss.cache.TreeCache;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads nodes that don't exist at the time of the call into memory from the CacheLoader. 
 * If the nodes were evicted earlier then we remove them from the cache loader after 
 * their attributes have been initialized and their children have been loaded in memory.
 * 
 * @author <a href="mailto:{hmesha@novell.com}">{Hany Mesha}</a>
 * @version $Id: ActivationInterceptor.java 2894 2006-11-10 15:48:38Z msurtani $
 */
public class ActivationInterceptor extends CacheLoaderInterceptor implements ActivationInterceptorMBean {
   
   protected TransactionManager tx_mgr=null;
   protected TransactionTable   tx_table=null;
   private HashMap m_txActivations = new HashMap();
   private long m_activations = 0;

   /** List<Transaction> that we have registered for */
   protected ConcurrentHashMap  transactions=new ConcurrentHashMap(16);
   protected static final Object NULL=new Object();

   public ActivationInterceptor() {
      this.useCacheStore = false;
   }
   
   /**
    * Makes sure a node is loaded into memory before a call executes. If node is 
    * already loaded and its attributes already initialized, then remove it from 
    * the cache loader and notify the cache listeners that the node has been activated.
    * 
    * @param call
    * @return
    * @throws Throwable
    */
   public Object invoke(MethodCall call) throws Throwable {
      JBCMethodCall m = (JBCMethodCall) call;
      Fqn          fqn=null;
      Method       meth=m.getMethod();
      Object[]     args=m.getArgs();
      Object       retval=null;

      // First call the parent class to load the node
      retval = super.invoke(m);
      
      // is this a node removal operation?
      boolean nodeRemoved = false;
      
      // Could be TRANSACTIONAL. If so, we register for TX completion (if we haven't done so yet)
      if(tx_mgr != null && tx_mgr.getTransaction() != null) {
         GlobalTransaction gtx = getInvocationContext().getGlobalTransaction();
         switch (m.getMethodId())
         {
            case MethodDeclarations.commitMethod_id:
               if (hasModifications(args)) {
                  loader.commit(gtx);
                  if (cache.getUseInterceptorMbeans()&& statsEnabled) {
                     Integer acts = (Integer)m_txActivations.get(gtx);
                     if (acts != null)
                        m_activations = m_activations + acts.intValue();
                     m_txActivations.remove(gtx);
                  }
               }
               break;
            case MethodDeclarations.rollbackMethod_id:
               if (hasModifications(args)) {
                  loader.rollback(gtx);
                  if (cache.getUseInterceptorMbeans()&& statsEnabled)
                     m_txActivations.remove(gtx);
               }
               break;
            case MethodDeclarations.optimisticPrepareMethod_id:
            case MethodDeclarations.prepareMethod_id:
               prepareCacheLoader(gtx);
               break;
         }
      }
      
      // if we're here then it's not transactional

      // CacheLoaderInterceptor normally doesn't load the node
      // since CacheStoreInterceptor.put() returns the old value
      switch (m.getMethodId())
      {
         case MethodDeclarations.putDataMethodLocal_id:
         case MethodDeclarations.putDataEraseMethodLocal_id:
         case MethodDeclarations.putKeyValMethodLocal_id:
            fqn=(Fqn)args[1];
            break;
         case MethodDeclarations.removeKeyMethodLocal_id:
         case MethodDeclarations.removeDataMethodLocal_id:
            fqn=(Fqn)args[1];
            break;
         case MethodDeclarations.addChildMethodLocal_id:
            fqn=(Fqn)args[1];
            break;
         case MethodDeclarations.getKeyValueMethodLocal_id:
            fqn=(Fqn)args[0];
            break;
         case MethodDeclarations.getNodeMethodLocal_id:
            fqn=(Fqn)args[0];
            break;
         case MethodDeclarations.getKeysMethodLocal_id:
            fqn=(Fqn)args[0];
            break;
         case MethodDeclarations.getChildrenNamesMethodLocal_id:
         case MethodDeclarations.releaseAllLocksMethodLocal_id:
         case MethodDeclarations.printMethodLocal_id:
            fqn=(Fqn)args[0];
            break;
         case MethodDeclarations.removeNodeMethodLocal_id:
            nodeRemoved = true;
            fqn=(Fqn)args[1];
            break;
      }
      
      synchronized(this) {
         if (fqn != null && nodeRemoved)
            // If the node is being removed, just remove it and don't perform
            // activation processing
            loader.remove(fqn);
         else if (fqn != null && cache.exists(fqn) && loader.exists(fqn)) {
            // Remove the node from the cache loader if it exists in memory,
            // its attributes have been initialized, its children have been loaded,
            // AND it was found in the cache loader (nodeLoaded = true). 
            // Then notify the listeners that the node has been activated.
            DataNode n = getNode(fqn); // don't load
            // node not null and attributes have been loaded?
            if (n != null && !n.containsKey(TreeCache.UNINITIALIZED)) {
               if (n.hasChildren()) {
                  if (allInitialized(n)) {
                     log.debug("children all initialized");
                     remove(fqn);
                  }
               } else if (loaderNoChildren(fqn)) {
                  log.debug("no children " + n);
                  remove(fqn);
               }
            }
         }
      }
      return retval;
   }

   private void remove(Fqn fqn) throws Exception {
      loader.remove(fqn);
      cache.notifyNodeActivate(fqn, false);
      if (cache.getUseInterceptorMbeans()&& statsEnabled)
         m_activations++;
   }

   /**
    * Returns true if a node has all children loaded and initialized.
    */ 
   private boolean allInitialized(DataNode n) {
      if (!n.getChildrenLoaded())
         return false;
      for (Iterator it=n.getChildren().values().iterator(); it.hasNext();) {
         DataNode child = (DataNode)it.next();
         if (child.containsKey(TreeCache.UNINITIALIZED))
            return false;
      }
      return true;
   }

   /**
    * Returns true if the loader indicates no children for this node.
    * Return false on error.
    */ 
   private boolean loaderNoChildren(Fqn fqn)
   {
      try {
         Set children_names = loader.getChildrenNames(fqn);
         return (children_names == null);
      }
      catch (Exception e) {
         log.error("failed getting the children names for " + fqn + " from the cache loader", e);
         return false;
      }
   }
   
   public long getActivations() {
      return m_activations;  
   }
   
   public void resetStatistics() {
      super.resetStatistics();
      m_activations = 0;
   }
   
   public Map dumpStatistics() {
      Map retval = super.dumpStatistics();
      if (retval == null)
         retval = new HashMap();
      retval.put("Activations", new Long(m_activations));
      return retval;
   }

   protected boolean hasModifications(Object[] args) {
      int hint = 1;
      if (args[hint] instanceof Boolean) return ((Boolean) args[hint]).booleanValue();
      for (int i = 0; i < args.length; i++) {
         if (args[i] instanceof Boolean) return ((Boolean) args[i]).booleanValue();
      }
      return false;
   }

   private void prepareCacheLoader(GlobalTransaction gtx) throws Exception {
      List modifications;
      TransactionEntry entry;
      int txActs = 0;
      
      entry=tx_table.get(gtx);
      if(entry == null)
         throw new Exception("entry for transaction " + gtx + " not found in transaction table");
      modifications=entry.getCacheLoaderModifications();
      if(modifications.size() == 0)
         return;
      List cache_loader_modifications=new ArrayList();
      for(Iterator it=modifications.iterator(); it.hasNext();) {
         JBCMethodCall methodCall=(JBCMethodCall)it.next();
         Method method=methodCall.getMethod();
         Object[] args;
         if(method == null)
            throw new Exception("method call has no method: " + methodCall);
         args=methodCall.getArgs();
         switch (methodCall.getMethodId()) 
         {
            case MethodDeclarations.removeNodeMethodLocal_id:
               // just remove it from loader, don't trigger activation processing
               Modification mod=new Modification(Modification.REMOVE_NODE, (Fqn)args[1]);
               cache_loader_modifications.add(mod);
               break;
            case MethodDeclarations.putDataMethodLocal_id:
            case MethodDeclarations.putDataEraseMethodLocal_id:
            case MethodDeclarations.putKeyValMethodLocal_id:
               // On the way out, remove the node from the cache loader.
               // Only remove the node if it exists in memory, its attributes have
               // been initialized, its children have been loaded
               // AND it was found in the cache loader (nodeLoaded = true). 
               // Then notify the listeners that the node has been activated.
                Fqn fqn = (Fqn)args[1];
                if(fqn != null && cache.exists(fqn)  && loader.exists(fqn)) {
                   DataNode n=getNode(fqn); // don't load
                   // node not null and attributes have been loaded?
                   if (n != null && !n.containsKey(TreeCache.UNINITIALIZED)) {
                      // has children?
                      if(n.hasChildren() && allInitialized(n)) {
                         // children have been loaded, remove the node
                         addRemoveMod(cache_loader_modifications, fqn);
                         txActs++;
                      }
                      // doesn't have children, check the cache loader
                      else if (loaderNoChildren(fqn)) {
                         addRemoveMod(cache_loader_modifications, fqn);
                         txActs++;
                      }
                   }
                }
                break;
         }
      }
      if (cache_loader_modifications.size() > 0) {
         loader.prepare(gtx, cache_loader_modifications, false);
         if (cache.getUseInterceptorMbeans() && statsEnabled && txActs > 0)
            m_txActivations.put(gtx, new Integer(txActs));
      }
   }

   private void addRemoveMod(List l, Fqn fqn) {
       Modification mod = new Modification(Modification.REMOVE_NODE, fqn);
       l.add(mod);
       cache.notifyNodeActivate(fqn, false);
   }

}
