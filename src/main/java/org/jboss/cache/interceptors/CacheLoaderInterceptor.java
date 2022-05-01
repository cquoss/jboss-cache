package org.jboss.cache.interceptors;

import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.TransactionEntry;
import org.jboss.cache.TransactionTable;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeNode;
import org.jboss.cache.config.Option;
import org.jboss.cache.loader.AsyncCacheLoader;
import org.jboss.cache.loader.CacheLoader;
import org.jboss.cache.loader.ChainingCacheLoader;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * Loads nodes that don't exist at the time of the call into memory from the CacheLoader
 *
 * @author Bela Ban
 * @version $Id: CacheLoaderInterceptor.java 4142 2007-07-13 19:36:25Z bstansberry $
 */
public class CacheLoaderInterceptor extends Interceptor implements CacheLoaderInterceptorMBean
{
   private boolean isCustomCacheLoader;
   private long m_cacheLoads = 0;
   private long m_cacheMisses = 0;
   private TransactionTable txTable = null;
   protected CacheLoader loader;

   /**
    * True if CacheStoreInterceptor is in place.
    * This allows us to skip loading keys for remove(Fqn, key) and put(Fqn, key).
    * It also affects removal of node data and listing children.
    */
   protected boolean useCacheStore = true;

   public void setCache(TreeCache cache)
   {
      super.setCache(cache);
      txTable = cache.getTransactionTable();
      this.loader = cache.getCacheLoaderManager().getCacheLoader();
      isCustomCacheLoader = isCustomCacheLoaderConfigured(loader);
   }

   private boolean isCustomCacheLoaderConfigured(CacheLoader cl)
   {
      if (cl instanceof ChainingCacheLoader)
      {
         // test all loaders in the chain.
         ChainingCacheLoader ccl = (ChainingCacheLoader) cl;
         Iterator it = ccl.getCacheLoaders().iterator();
         boolean isCustom = false;
         while (it.hasNext())
         {
            CacheLoader nextCacheLoader = (CacheLoader) it.next();
            isCustom = isCustom || isCustomCacheLoaderConfigured(nextCacheLoader);
         }
         return isCustom;
      }
      else if (cl instanceof AsyncCacheLoader)
      {
         // test the underlying cache loader
         CacheLoader underlying = ((AsyncCacheLoader) cl).getCacheLoader();
         return isCustomCacheLoaderConfigured(underlying);
      }
      else
      {
         // tests for org.jboss.cache.loader.*
         Package pkg = cl.getClass().getPackage(); // may be null if this is an inner class?  In which case it is certainly a custom cache loader instance.
         return pkg == null || !pkg.getName().startsWith("org.jboss.cache");
      }
   }

   /**
    * Makes sure a node is loaded into memory before a call executes (no-op if node is already loaded). If attributes
    * of a node are to be accessed by the method, the attributes are also loaded.
    *
    * @param call
    * @return
    * @throws Throwable
    */
   public Object invoke(MethodCall call) throws Throwable
   {
      JBCMethodCall m = (JBCMethodCall) call;
      Fqn fqn = null; // if set, load the data
      Method meth = m.getMethod();
      Object[] args = m.getArgs();
      boolean acquireLock = false; // do we need to acquire a lock if we load this node from cloader?
      Map nodeData = null;
      boolean initNode = false; // keep uninitialized
      Object key = null;
      InvocationContext ctx = getInvocationContext();
      TransactionEntry entry = null;
      GlobalTransaction gtx = null;
      if ((gtx = ctx.getGlobalTransaction()) != null)
      {
         entry = txTable.get(gtx);
      }

      if (log.isTraceEnabled())
         log.trace("invoke " + m);
      switch (m.getMethodId())
      {
         case MethodDeclarations.putDataEraseMethodLocal_id:
         case MethodDeclarations.putDataMethodLocal_id:
            fqn = (Fqn) args[1];
            initNode = true;
            break;
         case MethodDeclarations.putKeyValMethodLocal_id:
            fqn = (Fqn) args[1];
            if (useCacheStore)
               initNode = true;
            else
               acquireLock = true;
            break;
         case MethodDeclarations.addChildMethodLocal_id:
            fqn = (Fqn) args[1];
            break;
         case MethodDeclarations.getKeyValueMethodLocal_id:
            fqn = (Fqn) args[0];
            key = args[1];
            acquireLock = true;
            break;
         case MethodDeclarations.getNodeMethodLocal_id:
         case MethodDeclarations.getKeysMethodLocal_id:
         case MethodDeclarations.getChildrenNamesMethodLocal_id:
         case MethodDeclarations.releaseAllLocksMethodLocal_id:
         case MethodDeclarations.printMethodLocal_id:
            fqn = (Fqn) args[0];
            acquireLock = true;
            break;
         case MethodDeclarations.rollbackMethod_id:
            // clean up nodesCreated map
            cleanupNodesCreated(entry);
            break;
         default:
            if (!useCacheStore)
            {
               if (m.getMethodId() == MethodDeclarations.removeKeyMethodLocal_id)
               {
                  fqn = (Fqn) args[1];
               }
               else if (m.getMethodId() == MethodDeclarations.removeDataMethodLocal_id)
               {
                  fqn = (Fqn) args[1];
                  initNode = true;
               }
            }
            break;
      }

      /* On the way in: load elements into cache from the CacheLoader if not yet in the cache. We need to synchronize
      this so only 1 thread attempts to load a given element */

      if (fqn != null)
      {

         DataNode n = cache.peek(fqn);
         if (log.isTraceEnabled())
            log.trace("load element " + fqn + " mustLoad=" + mustLoad(n, key));
         if (mustLoad(n, key))
         {
            if (initNode)
            {
               n = createTempNode(fqn, entry);
            }
            // Only attempt to acquire this lock if we need to - i.e., if
            // the lock hasn't already been acquired by the Lock
            // interceptor.  CRUD methods (put, remove) would have acquired
            // this lock - even if the node is not in memory and needs to be
            // loaded.  Non-CRUD methods (put) would NOT have acquired this
            // lock so if we are to load the node from cache loader, we need
            // to acquire a write lock here.  as a 'catch-all', DO NOT
            // attempt to acquire a lock here *anyway*, even for CRUD
            // methods - this leads to a deadlock when you have threads
            // simultaneously trying to create a node.  See
            // org.jboss.cache.loader.deadlock.ConcurrentCreationDeadlockTest
            // - Manik Surtani (21 March 2006)
            if (acquireLock)
               lock(fqn, DataNode.LOCK_TYPE_WRITE, false); // not recursive

            if (!initNode && !wasRemovedInTx(fqn))
            {
               n = loadNode(fqn, n, entry);
            }
         }

         // The complete list of children aren't known without loading them
         if (m.getMethodId() == MethodDeclarations.getChildrenNamesMethodLocal_id)
         {
            loadChildren(fqn, n);
         }

      }

      return super.invoke(m);
   }

   /**
    * Load the children.
    *
    * @param n may be null if the node was not found.
    */
   private void loadChildren(Fqn fqn, DataNode n) throws Throwable
   {

      if (n != null && n.getChildrenLoaded())
         return;
      Set children_names = loader.getChildrenNames(fqn);

      if (log.isTraceEnabled())
         log.trace("load children " + fqn + " children=" + children_names);

      // For getChildrenNames null means no children
      if (children_names == null)
      {
         if (n != null)
         {
            if (useCacheStore)
               n.setChildren(null);
            n.setChildrenLoaded(true);
         }
         return;
      }

      // Create if node had not been created already
      if (n == null)
         n = createNodes(fqn, null); // dont care about local transactions

      // Create one DataNode per child, mark as UNINITIALIZED
      for (Iterator i = children_names.iterator(); i.hasNext();)
      {
         String child_name = (String) i.next();
         Fqn child_fqn = new Fqn(fqn, child_name);
         // create child if it didn't exist
         n.createChild(child_name, child_fqn, n, TreeCache.UNINITIALIZED, null);
      }
      lock(fqn, DataNode.LOCK_TYPE_READ, true); // recursive=true: lock entire subtree
      n.setChildrenLoaded(true);
   }

   private boolean mustLoad(DataNode n, Object key)
   {
      return n == null ||
              (n.containsKey(TreeCache.UNINITIALIZED) && (key == null || !n.containsKey(key)));
   }

   public long getCacheLoaderLoads()
   {
      return m_cacheLoads;
   }

   public long getCacheLoaderMisses()
   {
      return m_cacheMisses;
   }

   public void resetStatistics()
   {
      m_cacheLoads = 0;
      m_cacheMisses = 0;
   }

   public Map dumpStatistics()
   {
      Map retval = new HashMap();
      retval.put("CacheLoaderLoads", new Long(m_cacheLoads));
      retval.put("CacheLoaderMisses", new Long(m_cacheMisses));
      return retval;
   }

   protected void lock(Fqn fqn, int lock_type, boolean recursive) throws Throwable
   {

      if (cache.isNodeLockingOptimistic()) return;

      MethodCall meth = MethodCallFactory.create(MethodDeclarations.lockMethodLocal,
              new Object[]{fqn,
                      new Integer(lock_type),
                      Boolean.valueOf(recursive)});
      //super.invoke(meth);
      // let's force this to go thru the whole chain, not just from here on.
      // TxInterceptor will scrub the InvocationContext of any Option, so
      // cache it so we can restore it
      InvocationContext ctx = getInvocationContext();
      Option opt = ctx.getOptionOverrides();
      try
      {
         ((Interceptor) cache.getInterceptors().get(0)).invoke(meth); // need a better way to do this
      }
      finally
      {
         if (opt != null)
            ctx.setOptionOverrides(opt);
      }
   }

   /**
    * Retrieves a node from memory; doesn't access the cache loader
    *
    * @param fqn
    */
   protected DataNode getNode(Fqn fqn)
   {
      int treeNodeSize = fqn.size();

      TreeNode n = cache.getRoot();
      TreeNode child_node;
      Object child_name;
      for (int i = 0; i < treeNodeSize && n != null; i++)
      {
         child_name = fqn.get(i);
         child_node = n.getChild(child_name);
         n = child_node;
      }
      return (DataNode) n;
   }

   /**
    * Returns true if the FQN or parent was removed during the current
    * transaction.
    * This is O(N) WRT to the number of modifications so far.
    */
   private boolean wasRemovedInTx(Fqn fqn)
   {
      GlobalTransaction t = getInvocationContext().getGlobalTransaction();
      if (t == null)
         return false;
      TransactionEntry entry = txTable.get(t);
      Iterator i = entry.getCacheLoaderModifications().iterator();
      while (i.hasNext())
      {
         JBCMethodCall m = (JBCMethodCall) i.next();
         if (m.getMethodId() == MethodDeclarations.removeNodeMethodLocal_id
                 && fqn.isChildOrEquals((Fqn) m.getArgs()[1]))
            return true;
      }
      return false;
   }

   /**
    * Loads a node from disk; if it exists creates parent TreeNodes.
    * If it doesn't exist on disk but in memory, clears the
    * uninitialized flag, otherwise returns null.
    */
   private DataNode loadNode(Fqn fqn, DataNode n, TransactionEntry entry) throws Exception
   {
      if (log.isTraceEnabled()) log.trace("loadNode " + fqn);
      Map nodeData = loadData(fqn);
      if (nodeData != null)
      {
         n = createNodes(fqn, entry);
         n.put(nodeData, true);
      }
      else if (n != null && n.containsKey(TreeCache.UNINITIALIZED))
      {
         n.remove(TreeCache.UNINITIALIZED);
      }
      return n;
   }

   /**
    * Creates a new memory node in preparation for storage.
    */
   private DataNode createTempNode(Fqn fqn, TransactionEntry entry) throws Exception
   {
      DataNode n = createNodes(fqn, entry);
      n.put(TreeCache.UNINITIALIZED, null);
      if (log.isTraceEnabled())
         log.trace("createTempNode n " + n);
      return n;
   }

   private DataNode createNodes(Fqn fqn, TransactionEntry entry) throws Exception
   {
      Fqn tmp_fqn = Fqn.ROOT;

      int size = fqn.size();

      TreeNode n = cache.getRoot();
      for (int i = 0; i < size; i++)
      {
         Object child_name = fqn.get(i);
         tmp_fqn = new Fqn(tmp_fqn, child_name);
         TreeNode child_node = n.getChild(child_name);
         boolean last = (i == size - 1);

         if (child_node == null)
         {
            if (last)
            {
               child_node = n.createChild(child_name, tmp_fqn, n);
            }
            else
            {
               child_node = n.createChild(child_name, tmp_fqn, n, TreeCache.UNINITIALIZED, null);
            }

            if (entry != null)
            {
               entry.loadUninitialisedNode(tmp_fqn);
            }
         }

         n = child_node;
      }

      return (DataNode) n;
   }

   private void cleanupNodesCreated(TransactionEntry entry)
   {
      boolean traceEnabled = log.isTraceEnabled();
      log.trace("Removing temporarily created nodes from treecache");

      // this needs to be done in reverse order.
      List list = entry.getDummyNodesCreatedByCacheLoader();
      if (list != null && list.size() > 0)
      {
         ListIterator i = list.listIterator(list.size());
         while (i.hasPrevious())
         {
            Fqn fqn = (Fqn) i.previous();
            try
            {
               cache._evict(fqn);
            }
            catch (CacheException e)
            {
               if (traceEnabled) log.trace("Unable to evict node " + fqn, e);
            }
         }
      }
   }


   private Map loadData(Fqn fqn) throws Exception
   {
      Map nodeData = loader.get(fqn);
      boolean nodeExists = (nodeData != null);
      if (log.isTraceEnabled()) log.trace("nodeExists " + nodeExists);

      if (cache.getUseInterceptorMbeans() && statsEnabled)
      {
         if (nodeExists)
            m_cacheLoads++;
         else
            m_cacheMisses++;
      }

      if (!nodeExists && isCustomCacheLoader)
      {
         warnCustom();
      }

      if (nodeExists)
      {
         cache.notifyNodeLoaded(fqn);
      }

      return nodeData;
   }

   private void warnCustom()
   {
      log.info("CacheLoader.get(Fqn) returned a null; assuming the node does not exist.");
      log.info("The CacheLoader interface has changed since JBossCache 1.3.x");
      log.info("Please see http://jira.jboss.com/jira/browse/JBCACHE-118");
      log.info("CacheLoader.get() should return an empty Map if the node does exist but doesn't have any attributes.");
   }

}
