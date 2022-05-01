/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jboss.aop.InstanceAdvisor;
import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.RegionNotEmptyException;
import org.jboss.cache.TreeCache;
import org.jboss.cache.aop.eviction.AopEvictionPolicy;
import org.jboss.cache.aop.util.ObjectUtil;
import org.jboss.cache.lock.UpgradeException;
import org.jboss.cache.marshall.ObjectSerializationFactory;
import org.jboss.cache.marshall.Region;
import org.jboss.cache.marshall.RegionNameConflictException;
import org.jboss.cache.transaction.BatchModeTransactionManager;
import org.jboss.cache.xml.XmlHelper;
import org.jgroups.JChannel;
import org.w3c.dom.Element;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * PojoCache implementation class. User should use the {@link PojoCacheIfc} interface directly to
 * access the public APIs.
 * <p>PojoCache is an in-memory, transactional, fine-grained, and object-oriented cache system. It
 * differs from the typical generic cache library by operating on the pojo level directly without requiring
 * that object to be serializable (although it does require "aspectizing" the POJO). Further, it can
 * preserve object graph during replication or persistency. It also track the replication via fine-grained
 * maner, i.e., only changed fields are replicated.</p>
 *
 * @author Ben Wang
 * @since 1.4
 */
public class PojoCache extends TreeCache implements PojoCacheMBean
{
   // Class -> CachedType
   // use WeakHashMap to allow class reloading
   protected Map cachedTypes = new WeakHashMap();
   // Use batch mode tm to simulate the batch processing.
   TransactionManager localTm_ = BatchModeTransactionManager.getInstance();
   protected TreeCacheAopDelegate delegate_;
   Element config_ = null;
   protected final String LOCK = "_lock_";
   protected final int RETRY = 5; // retry times for lockPojo just in case there is upgrade exception during concurrent access.
//   boolean detachPojoWhenEvicted_ = false;
   protected boolean marshallNonSerializable_ = false;
   protected ThreadLocal undoListLocal_ = new ThreadLocal();
   protected ThreadLocal hasSynchronizationHandler_ = new ThreadLocal();

   public PojoCache(String cluster_name,
                    String props,
                    long state_fetch_timeout)
         throws Exception
   {
      super(cluster_name, props, state_fetch_timeout);
      init();
   }

   public PojoCache() throws Exception
   {
      init();
   }

   public PojoCache(JChannel channel) throws Exception
   {
      super(channel);
      init();
   }

   protected void init()
   {
      delegate_ = new TreeCacheAopDelegate(this);
   }

   public void startService() throws Exception
   {
      super.startService();
      parseConfig();
   }

   public void stopService()
   {
      super.stopService();
   }

   protected void parseConfig()
   {
      if (config_ == null)
      {
         log.info("parseConfig(): PojoCacheConfig is empty");
         return;
      }
      marshallNonSerializable_ = XmlHelper.readBooleanContents(config_, "marshallNonSerializable");
      log.info("marshallNonSerializable flag is set: " +marshallNonSerializable_);

//      detachPojoWhenEvicted_ = XmlHelper.readBooleanContents(config_, "DetachPojoWhenEvicted");
   }

   /**
    * Over-ride to make sure we are using an eviction policy specific to aop.
    */
   public void setEvictionPolicyClass(String eviction_policy_class)
   {
      this.eviction_policy_class = eviction_policy_class;
      if (eviction_policy_class == null || eviction_policy_class.length() == 0)
         return;

      try
      {
         Object obj = loadClass(eviction_policy_class).newInstance();
         if (! (obj instanceof AopEvictionPolicy))
            throw new RuntimeException("PojoCache.setEvictionPolicyClass(). Eviction policy provider:" +
                  eviction_policy_class + " is not an instance of AopEvictionPolicy.");
         super.setEvictionPolicyClass(eviction_policy_class);
      }
      catch (RuntimeException ex)
      {
         log.error("setEvictionPolicyClass(): failed creating instance of  " + eviction_policy_class, ex);
         throw ex;
      }
      catch (Throwable t)
      {
         log.error("setEvictionPolicyClass(): failed creating instance of  " + eviction_policy_class, t);
      }
   }

   public void addUndoInterceptor(InstanceAdvisor advisor, BaseInterceptor interceptor, int op)
   {
      List list = (List)undoListLocal_.get();
      if(list == null)
      {
         list = new ArrayList();
         undoListLocal_.set(list);
      }
      ModificationEntry ent = new ModificationEntry(advisor, interceptor, op);
      list.add(ent);
   }

   public void addUndoCollectionProxy(Field field, Object key, Object oldValue)
   {
      List list = (List)undoListLocal_.get();
      if(list == null)
      {
         list = new ArrayList();
         undoListLocal_.set(list);
      }
      ModificationEntry ent = new ModificationEntry(field, key, oldValue);
      list.add(ent);
   }

   public void resetUndoOp()
   {
      List list = (List)undoListLocal_.get();
      if(list != null)
         list.clear();
      hasSynchronizationHandler_.set(null);
   }

   public List getModList()
   {
      // No need to make it unmodifiable since this is thread local
      return (List)undoListLocal_.get();
   }

   /**
    * Override to provide aop specific eviction.
    * <p/>
    * <p/>
    * Called by eviction policy provider. Note that eviction is done only in local mode,
    * that is, it doesn't replicate the node removal. This will cause the replication nodes
    * not synchronizing, but it is ok since user is supposed to add the node again when get is
    * null. After that, the contents will be in sync.
    *
    * @param fqn Will remove everythign assoicated with this fqn.
    * @throws org.jboss.cache.CacheException
    */
   public void evict(Fqn fqn) throws CacheException
   {
      // We will remove all children nodes as well since we assume all children nodes are part
      // of this "object" node.
      if (delegate_.isAopNode(fqn))
      {
         if (log.isDebugEnabled())
         {
            log.debug("evict(): evicting whole aop node " + fqn);
         }
//         _evictObject(fqn);
         recursiveEvict(fqn);
      }
      else
      {
         super.evict(fqn);
      }
   }

   void recursiveEvict(Fqn fqn) throws CacheException
   {
      boolean create_undo_ops = true;
      boolean sendNodeEvent = false;
      // Let's do it brute force.
      _remove(null, fqn, create_undo_ops, sendNodeEvent);

      // since this is not in the scope of a tx, do a realRemove() immediately - Manik, 1/Dec/2006 - JBCACHE-871
      realRemove(fqn, false);      
   }

   /**
    * Package level evict for plain cache.
    *
    * @param fqn
    * @throws CacheException
    */
   void plainEvict(Fqn fqn) throws CacheException
   {
      super.evict(fqn);
   }

   protected void createEvictionPolicy()
   {
      super.createEvictionPolicy();
      this.evictionInterceptorClass = "org.jboss.cache.aop.interceptors.PojoEvictionInterceptor";
   }

   protected void _evictSubtree(Fqn subtree) throws CacheException
   {

      if (log.isTraceEnabled())
         log.trace("_evictSubtree(" + subtree + ")");

      //    We will remove all children nodes as well since we assume all children nodes are part
      // of this "object" node.
      if (delegate_.isAopNode(subtree))
      {
         if (log.isDebugEnabled())
         {
            log.debug("evict(): evicting whole aop node " + subtree);
         }
//         _evictObject(subtree);
         recursiveEvict(subtree);
      }
      else
      {
         super._evictSubtree(subtree);
      }

   }

   /**
    * Overrides the {@link TreeCache#activateRegion(String) superclass method} by
    * ensuring that the internal region where information on shared object is stored
    * has been activated.
    */
   public void activateRegion(String subtreeFqn) throws RegionNotEmptyException, RegionNameConflictException, CacheException
   {
      if (!useRegionBasedMarshalling)
         throw new IllegalStateException("TreeCache.activateRegion(). useRegionBasedMarshalling flag is not set!");

      if ("/".equals(subtreeFqn))
      {
         // Just pass it through, as we'll get the internal area
         // with the normal state transfer
         super.activateRegion(subtreeFqn);
      }
      else
      {
         // If the internal region is not activated yet, activate it first
         Region region = regionManager_.getRegion(InternalDelegate.JBOSS_INTERNAL);
         if ((region == null && inactiveOnStartup)
               || (region != null && region.isInactive()))
         {
            super.activateRegion(InternalDelegate.JBOSS_INTERNAL.toString());
         }

         // If we don't have an internal map node yet, create one.
         // Doing this ensures the code that integrates map references for
         // the region will have a node to integrate into
         if (get(InternalDelegate.JBOSS_INTERNAL_MAP) == null)
            createSubtreeRootNode(InternalDelegate.JBOSS_INTERNAL_MAP);

         // Now activate the requested region
         super.activateRegion(subtreeFqn);
      }
   }

   /**
    * Overrides the superclass version by additionally acquiring locks
    * on the internal reference map nodes used for tracking shared objects.
    */
   protected void acquireLocksForStateTransfer(DataNode root,
                                               Object lockOwner,
                                               long timeout,
                                               boolean force)
         throws Exception
   {
      super.acquireLocksForStateTransfer(root, lockOwner, timeout, true, force);
      Fqn fqn = root.getFqn();
      if (fqn.size() > 0 &&
            !fqn.isChildOf(InternalDelegate.JBOSS_INTERNAL))
      {
         DataNode refMapNode = get(InternalDelegate.JBOSS_INTERNAL_MAP);
         if (refMapNode != null)
         {

            // Lock the internal map node but not its children to
            // prevent the addition of other children
            super.acquireLocksForStateTransfer(refMapNode, lockOwner, timeout,
                  false, force);

            // Walk through the children, and lock any whose name starts
            // with the string version of our root node's Fqn
            Map children = refMapNode.getChildren();
            if (children != null)
            {

               String targetFqn = ObjectUtil.getIndirectFqn(fqn);

               Map.Entry entry;
               for (Iterator iter = children.entrySet().iterator();
                    iter.hasNext();)
               {
                  entry = (Map.Entry) iter.next();
                  if (((String) entry.getKey()).startsWith(targetFqn))
                  {
                     super.acquireLocksForStateTransfer((DataNode) entry.getValue(),
                           lockOwner, timeout,
                           false, force);
                  }
               }
            }
         }
      }
   }

   /**
    * Overrides the superclass version by additionally releasing locks
    * on the internal reference map nodes used for tracking shared objects.
    */
   protected void releaseStateTransferLocks(DataNode root, Object lockOwner)
   {
      boolean releaseInternal = true;
      try
      {
         super.releaseStateTransferLocks(root, lockOwner, true);
         Fqn fqn = root.getFqn();
         releaseInternal = (fqn.size() > 0 &&
               !fqn.isChildOf(InternalDelegate.JBOSS_INTERNAL));
      }
      finally
      {
         if (releaseInternal)
         {
            try
            {
               DataNode refMapNode = get(InternalDelegate.JBOSS_INTERNAL_MAP);
               if (refMapNode != null)
               {
                  // Rather than going to the effort of identifying which
                  // child nodes we locked before, just release all children
                  super.releaseStateTransferLocks(refMapNode, lockOwner, true);
               }
            }
            catch (CacheException ce)
            {
               log.error("Caught exception releasing locks on internal RefMap", ce);
            }
         }
      }
   }

   /**
    * Obtain a cache aop type for user to traverse the defined "primitive" types in aop.
    * Note that this is not a synchronized call now for speed optimization.
    *
    * @param clazz The original pojo class
    * @return CachedType
    */
   public synchronized CachedType getCachedType(Class clazz)
   {
      CachedType type = (CachedType) cachedTypes.get(clazz);
      if (type == null)
      {
         type = new CachedType(clazz);
         cachedTypes.put(clazz, type);
         return type;
      } else
      {
         return type;
      }
   }

   /**
    */
   public Object getObject(String fqn) throws CacheException
   {
      return getObject(Fqn.fromString(fqn));
   }

   /**
    */
   public Object getObject(Fqn fqn) throws CacheException
   {
      return _getObject(fqn);
   }

   /**
    */
   public Object putObject(String fqn, Object obj) throws CacheException
   {
      return putObject(Fqn.fromString(fqn), obj);
   }

   /**
    */
   public Object putObject(Fqn fqn, Object obj) throws CacheException
   {
      checkFqnValidity(fqn);
      if(log.isDebugEnabled())
      {
         log.debug("putObject(): Fqn:" +fqn);
      }

      Object owner = null;
      if (hasCurrentTransaction())  // We have a transaction context going on now.
      {
         // Start a new transaction, we need transaction so the operation is batch.
         owner = getOwnerForLock(); // lock it for the whole duration of batch mode.
         // Lock the parent, create and add the child
         if(!lockPojo(owner, fqn))
         {
            throw new CacheException("PojoCache.putObject(): Can't obtain the pojo lock under fqn: "+fqn);
         }
         return _putObject(fqn, obj);
      }
      else
      {
         // Start a new transaction, we need transaction so the operation is batch.
         try
         {
            // Need this just in case the node does yet exist.

            localTm_.begin();
            owner = getOwnerForLock(); // lock it for the whole duration of batch mode.
            // Lock the parent, create and add the child
            if(!lockPojo(owner, fqn))
            {
               throw new CacheException("PojoCache.putObject(): Can't obtain the pojo lock under fqn: "+fqn);
            }
            Object objOld = _putObject(fqn, obj);
            return objOld;
         }
         catch (Exception e)
         {
            log.warn("putObject(): exception occurred: " +e);
            try
            {
               localTm_.setRollbackOnly();
            }
            catch (Exception exn)
            {
               exn.printStackTrace();
            }

            if (e instanceof RuntimeException)
               throw (RuntimeException) e;
            else if (e instanceof CacheException)
               throw (CacheException) e;
            else
               throw new RuntimeException("PojoCache.putObject(): fqn: " +fqn , e);
         }
         finally
         {
            endTransaction(fqn);
            // Release no matter what.
//            releasePojo(owner, fqn);
         }
      }
   }

   /**
    */
   public Object removeObject(String fqn) throws CacheException
   {
      return removeObject(Fqn.fromString(fqn));
   }

   /**
    */
   public Object removeObject(Fqn fqn) throws CacheException
   {
      checkFqnValidity(fqn);

      if(log.isDebugEnabled())
      {
         log.debug("removeObject(): Fqn:" +fqn);
      }

      Object owner = null;
      if (hasCurrentTransaction())  // We have a transaction context going on now.
      {
         owner = getOwnerForLock();
         if(!lockPojo(owner, fqn))
         {
            throw new CacheException("PojoCache.removeObject(): Can't obtain the pojo lock under fqn: "+fqn);
         }
         return _removeObject(fqn, true);
      }
      else
      {
         // Start a new transaction, we need transaction so the operation is atomic.
         try
         {
            localTm_.begin();
            owner = getOwnerForLock();
            if(!lockPojo(owner, fqn))
            {
               throw new CacheException("PojoCache.removeObject(): Can't obtain the pojo lock under fqn: "+fqn);
            }
            return _removeObject(fqn, true);
         }
         catch (Exception e)
         {
            log.warn("removeObject(): exception occurred: " +e);
            try
            {
               localTm_.setRollbackOnly();
            }
            catch (Exception exn)
            {
               exn.printStackTrace();
            }

            if (e instanceof RuntimeException)
               throw (RuntimeException) e;
            else if (e instanceof CacheException)
               throw (CacheException) e;
            else
               throw new RuntimeException("PojoCache.removeObject(): fqn: " +fqn , e);
         }
         finally
         {
            endTransaction(fqn);
            // Release no matter what.
//            releasePojo(owner, fqn);
         }
      }
   }

   /**
    */
   public Map findObjects(String fqn) throws CacheException
   {
      return findObjects(Fqn.fromString(fqn));
   }

   /**
    */
   public Map findObjects(Fqn fqn) throws CacheException
   {
      return _findObjects(fqn);
   }

   public void setMarshallNonSerializable(boolean marshall)
   {
      if(marshall)
      {
         if(!ObjectSerializationFactory.useJBossSerialization())
         {
            throw new IllegalStateException("PojoCache.setMarshallNonSerializable(). " +
            "Can't set MarshallNonSerializable to true since useJBossSerialization is false");
         }
      }
      marshallNonSerializable_ = marshall;
   }

   public boolean isMarshallNonSerializable()
   {
      return marshallNonSerializable_;
   }


   /**
    * Inject the config element that is specific to PojoCache.
    *
    * @param config
    * @throws CacheException
    */
   public void setPojoCacheConfig(Element config) throws CacheException
   {
      config_ = config;
   }

   public Element getPojoCacheConfig()
   {
      return config_;
   }

   private void checkFqnValidity(Fqn fqn)
   {
      // throws exception is fqn is JBossInternal
      if (fqn.equals(InternalDelegate.JBOSS_INTERNAL))
      {
         throw new IllegalArgumentException("checkFqnValidity(): fqn is not valid: " + fqn);
      }
   }

   protected boolean lockPojo(Object owner, Fqn fqn) throws CacheException {
      if(log.isDebugEnabled())
      {
         log.debug("lockPojo(): Fqn:" +fqn + " Owner: " +owner);
      }

      boolean isNeeded = true;
      int retry = 0;
      while(isNeeded)
      {
         try
         {
            put(fqn, LOCK, "LOCK");
            isNeeded = false;
         } catch (UpgradeException upe)
         {
            log.warn("lockPojo(): can't upgrade the lock during lockPojo. Will re-try. Fqn: " +fqn
                    + " retry times: " +retry);
            get(fqn).release(owner);
            if(retry++ > RETRY)
            {
               return false;
            }
            // try to sleep a little as well.
            try {
               Thread.sleep(10);
            } catch (InterruptedException e) {
               ;
            }
            continue;
         }
      }

      return true;
   }

   protected void releasePojo(Object owner, Fqn fqn) throws CacheException {
      Node node = get(fqn);
      if(node == null)
      {
         if(log.isDebugEnabled())
         {
            log.debug("releasePojo(): node could have been released already.");
         }
         return;
      }

      node.release(owner);
   }

   protected boolean hasCurrentTransaction()
   {
      try
      {
         if (getCurrentTransaction() != null || localTm_.getTransaction() != null)
         {
            // We have transaction context. Return null to signify don't do anything
            return true;
         }
      } catch (SystemException e)
      {
         throw new RuntimeException("PojoCache.hasCurrentTransaction: ", e);
      }
      return false;
   }

   protected void endTransaction(Fqn fqn)
   {
      if (localTm_ == null)
      {
         log.warn("PojoCache.endTransaction(): tm is null for fqn: " +fqn);
         return;
      }


      try
      {
         if (localTm_.getTransaction().getStatus() != Status.STATUS_MARKED_ROLLBACK)
         {
            localTm_.commit();
         }
         else if(localTm_.getTransaction().getStatus() == Status.STATUS_ROLLEDBACK)
         {
            log.info("PojoCache.endTransaction(): has been rolled back for fqn: " +fqn);
         }
         else
         {
            log.info("PojoCache.endTransaction(): rolling back tx for fqn: " +fqn);
            localTm_.rollback();
         }
      }
      catch (RollbackException re)
      {
         // Do nothing here since cache may rollback automatically.
         log.warn("PojoCache.endTransaction(): rolling back transaction with exception: " +re);
      }
      catch (Exception e)
      {
         log.warn("PojoCache.endTransaction(): Failed with exception: " +e);
      }
   }

   /**
    *  Used by internal implementation. Not for general public.
    */
   public Object _getObject(Fqn fqn) throws CacheException
   {
      return delegate_._getObject(fqn);
   }

   /**
    *  Used by internal implementation. Not for general public.
    */
   public Object _putObject(Fqn fqn, Object obj) throws CacheException
   {
      registerTxHandler();
      return delegate_._putObject(fqn, obj);
   }

   protected void registerTxHandler() throws CacheException
   {
      try {
         // Need to have this in case of rollback
         Boolean isTrue = (Boolean)hasSynchronizationHandler_.get();
         if(isTrue == null || !isTrue.booleanValue())
         {
            Transaction tx = getLocalTransaction();
            if(tx == null)
            {
               tx = localTm_.getTransaction();
            }

            if(tx == null)
            {
               throw new IllegalStateException("PojoCache.registerTxHanlder(). Can't have null tx handle.");
            }
            tx.registerSynchronization(
                 new PojoTxSynchronizationHandler(tx, this));

            hasSynchronizationHandler_.set(Boolean.TRUE);
         }
      } catch (RollbackException e) {
         throw new CacheException("_putObject(). Exception: " +e);
      } catch (SystemException e) {
         throw new CacheException("_putObject(). Exception: " +e);
      }
   }

   /**
    *  Used by internal implementation. Not for general public.
    */
   public Object _removeObject(Fqn fqn) throws CacheException
   {
      boolean removeCacheInterceptor = true;
      return _removeObject(fqn, removeCacheInterceptor);
   }

   /**
    *  Used by internal implementation. Not for general public.
    */
   public Object _removeObject(Fqn fqn, boolean removeCacheInterceptor) throws CacheException
   {
      boolean evict = false;
      // Don't trigger bulk remove now since there is still some problem with Collection class
      // when it is detached.
      delegate_.setBulkRemove(true);
      registerTxHandler();
      return delegate_._removeObject(fqn, removeCacheInterceptor, evict);
   }

   /**
    *  Used by internal implementation. Not for general public.
    */
   public Object _evictObject(Fqn fqn) throws CacheException
   {
      boolean evict = true;
      boolean removeCacheInterceptor = false;

      // Configurable option to see if we want to remove the cache interceptor when the pojo is
      // evicted.
//      if(detachPojoWhenEvicted_) removeCacheInterceptor = true;
      delegate_.setBulkRemove(false);
      return delegate_._removeObject(fqn, removeCacheInterceptor, evict);
   }

   /**
    *  Used by internal implementation. Not for general public.
    */
   public Map _findObjects(Fqn fqn) throws CacheException
   {
      return delegate_._findObjects(fqn);
   }
}
