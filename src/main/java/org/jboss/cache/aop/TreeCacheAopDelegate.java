/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jboss.aop.Advised;
import org.jboss.aop.ClassInstanceAdvisor;
import org.jboss.aop.InstanceAdvisor;
import org.jboss.aop.Advisor;
import org.jboss.aop.proxy.ClassProxy;
import org.jboss.aop.advice.Interceptor;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.aop.util.AopUtil;
import org.jboss.cache.aop.util.SecurityActions;
import org.jboss.cache.aop.collection.AbstractCollectionInterceptor;
import org.jboss.cache.aop.collection.CollectionInterceptorUtil;
import org.jboss.cache.Fqn;
import org.jboss.cache.CacheException;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.TreeNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.blocks.MethodCall;

import java.lang.reflect.Field;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Collection;

/**
 * Delegate class for PojoCache.
 * @author Ben Wang
 */
public class TreeCacheAopDelegate
{
   protected PojoCache cache_;
   protected final static Log log=LogFactory.getLog(TreeCacheAopDelegate.class);
   protected InternalDelegate internal_;
   protected ObjectGraphHandler graphHandler_;
   protected CollectionClassHandler collectionHandler_;
   protected SerializableObjectHandler serializableHandler_;
   // Use ThreadLocal to hold a boolean isBulkRemove
   protected ThreadLocal bulkRemove_ = new ThreadLocal();
   protected final String DETACH = "DETACH";

   public TreeCacheAopDelegate(PojoCache cache)
   {
      cache_ = cache;
      internal_ = new InternalDelegate(cache);
      graphHandler_ = new ObjectGraphHandler(cache_, internal_, this);
      collectionHandler_ = new CollectionClassHandler(cache_, internal_, graphHandler_);
      serializableHandler_ = new SerializableObjectHandler(cache_, internal_);
   }

   public void setBulkRemove(boolean bulk)
   {
      bulkRemove_.set(Boolean.valueOf(bulk));
   }

   public boolean getBulkRemove()
   {
      return ((Boolean)bulkRemove_.get()).booleanValue();
   }

   protected Object _getObject(Fqn fqn) throws CacheException
   {
      // TODO Must we really to couple with BR? JBCACHE-669
      Object pojo = internal_.getPojoWithGravitation(fqn);
      if ( pojo != null) {
         // we already have an advised instance
         return pojo;
      }

      // OK. So we are here meaning that this is a failover or passivation since the transient
      // pojo instance is not around. Let's also make sure the right classloader is used
      // as well.
      ClassLoader prevCL = Thread.currentThread().getContextClassLoader();
      try
      {
         if (cache_.getRegionManager() != null)
         {
            cache_.getRegionManager().setUnmarshallingClassLoader(fqn);
         }
         return _getObjectInternal(fqn);
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(prevCL);
      }

   }

   protected Object _getObjectInternal(Fqn fqn) throws CacheException
   {
      // the class attribute is implicitly stored as an immutable read-only attribute
      Class clazz = internal_.peekAopClazz(fqn);
      //  clazz and aopInstance can be not null if this node is the replicated brother node.
      if (clazz == null)
         return null;

      /**
       * Reconstruct the managed POJO
       */
      CachedType type = cache_.getCachedType(clazz);
      Object obj;

      // Check for both Advised and Collection classes for object graph.
      if( (obj = graphHandler_.objectGraphGet(fqn)) != null )
         return obj; // retrieved from internal ref node. We are done.

      AOPInstance aopInstance = internal_.getAopInstance(fqn);
      if(aopInstance == null)
      {
         throw new RuntimeException("TreeCacheAopDelegate._getObject(): null AOPInstance.");
      }

      if (Advised.class.isAssignableFrom(clazz)) {
         try {
            Constructor ctr = clazz.getDeclaredConstructor(null);
            SecurityActions.setAccessible(ctr);
            obj = ctr.newInstance(null);
            // TODO Need to populate the object from the cache as well.
         }
         catch(Exception e) {
            throw new CacheException("failed creating instance of " + clazz.getName(), e);
         }
         // Insert interceptor at runtime
         InstanceAdvisor advisor = ((Advised) obj)._getInstanceAdvisor();
         CacheInterceptor interceptor = new CacheInterceptor(cache_, fqn, type);
         interceptor.setAopInstance(aopInstance);
         advisor.appendInterceptor(interceptor);
         cache_.addUndoInterceptor(advisor, interceptor, ModificationEntry.INTERCEPTOR_ADD);

      } else { // Must be Collection classes. We will use aop.ClassProxy instance instead.
         try {
            if( (obj = collectionHandler_.collectionObjectGet(fqn, clazz)) != null ) {
            } else {
               // Maybe it is just a serialized object.
               obj=serializableHandler_.serializableObjectGet(fqn);
            }
         }
         catch(Exception e) {
            throw new CacheException("failure creating proxy", e);
         }
      }

      internal_.setPojo(aopInstance, obj);
      return obj;
   }

   /**
    * Note that caller of this method will take care of synchronization within the <code>fqn</code> sub-tree.
    * @param fqn
    * @param obj
    * @return
    * @throws CacheException
    */
   protected Object _putObject(Fqn fqn, Object obj) throws CacheException
   {
      if(!cache_.isMarshallNonSerializable())
         AopUtil.checkObjectType(obj);

      if (obj == null) {
         return cache_._removeObject(fqn, true);
      }
      // Skip some un-necessary update if obj is the same class as the old one
      Object oldValue = internal_.getPojo(fqn);
      if (oldValue == obj && (obj instanceof Advised || obj instanceof ClassProxy)) return obj;  // value already in cache. return right away.

      //if(oldValue != null)
      //{
         // Trigger bulk remove here for performance
         setBulkRemove(true);
         oldValue = cache_._removeObject(fqn, true); // remove old value before overwriting it.
      //}

      // Remember not to print obj here since it will trigger the CacheInterceptor.
      if(log.isDebugEnabled()) {
         log.debug("putObject(): fqn: " + fqn);
      }

      // store object in cache
      if (obj instanceof Advised) {
         CachedType type = cache_.getCachedType(obj.getClass());
         // add interceptor
         InstanceAdvisor advisor = ((Advised) obj)._getInstanceAdvisor();
         if(advisor == null)
            throw new RuntimeException("_putObject(): InstanceAdvisor is null for: " +obj);

         // Step Check for cross references
         Interceptor interceptor = AopUtil.findCacheInterceptor(advisor);
         // Let's check for object graph, e.g., multiple and circular references first
         if (graphHandler_.objectGraphPut(fqn, interceptor, type, obj)) { // found cross references
            return oldValue;
         }

         // We have a clean slate then.
         _regularPutObject(fqn, obj, advisor, type);

         /**
          * Handling collection classes here.
          * First check if obj has been aspectized? That is, if it is a ClassProxy or not.
          * If not, we will need to create a proxy first for the Collection classes
          */
      } else if (collectionHandler_.collectionObjectPut(fqn, obj)) {
        //
      } else if (serializableHandler_.serializableObjectPut(fqn, obj)) {
        // must be Serializable, including primitive types
      } else
      {
         // I really don't know what this is.
         throw new RuntimeException("putObject(): obj: " +obj + " type is not recognizable.");
      }

      return oldValue;
   }

   /**
    * Based on the pojo to perform a bulk remove recursively if there is no object graph
    * relationship for performance optimization.
    */
   protected boolean bulkRemove(Fqn fqn, Object obj) throws CacheException {
      // Check for cross-reference. If there is, we can't do bulk remove
      // map contains (pojo, cacheinterceptor) pair that needs to undo the the removal.
      return false;
      /*Map undoMap = new HashMap();
      if(pojoGraphMultipleReferenced(obj, undoMap))
      {
         undoInterceptorDetach(undoMap);
         return false;
      } else
      {
         cache_.remove(fqn); // interceptor has been removed so it is safe to do bulk remove now.
      }
      return true;*/
   }

   protected void detachInterceptor(InstanceAdvisor advisor, Interceptor interceptor,
                                    boolean detachOnly, Map undoMap)
   {
      if(!detachOnly)
      {
         advisor.removeInterceptor(interceptor.getName());
         undoMap.put(advisor, interceptor);
      } else
      {
         undoMap.put(DETACH, interceptor);
      }
   }

   protected void undoInterceptorDetach(Map undoMap)
   {
      for(Iterator it = undoMap.keySet().iterator(); it.hasNext();)
      {
         Object obj = it.next();

         if(obj instanceof InstanceAdvisor)
         {
            InstanceAdvisor advisor = (InstanceAdvisor)obj;
            BaseInterceptor interceptor = (BaseInterceptor)undoMap.get(advisor);

            if(interceptor == null)
            {
               throw new IllegalStateException("TreeCacheAopDelegate.undoInterceptorDetach(): null interceptor");
            }

            advisor.appendInterceptor(interceptor);
         } else
         {
            BaseInterceptor interceptor = (BaseInterceptor)undoMap.get(obj);
            boolean copyToCache = false;
            ((AbstractCollectionInterceptor)interceptor).attach(null, copyToCache);
         }
      }
   }

   /**
    * Check recursively if the pojo and its graph is multiple referenced. If it is, we can't
    * do a bulk remove.
    */
   protected boolean pojoGraphMultipleReferenced(Object obj, Map undoMap) throws CacheException {
      // store object in cache
      if (obj instanceof Advised) {
         CachedType type = cache_.getCachedType(obj.getClass());
         // add interceptor
         InstanceAdvisor advisor = ((Advised) obj)._getInstanceAdvisor();
         if(advisor == null)
            throw new RuntimeException("pojoGraphMultipleReferenced(): InstanceAdvisor is null for: " +obj);

         BaseInterceptor interceptor = (BaseInterceptor)AopUtil.findCacheInterceptor(advisor);
         // just in case
         if(interceptor == null)
         {
            return false;
         }
         AOPInstance aopInstance = interceptor.getAopInstance();
         // Check if there is cross referenced.
         if(aopInstance.getRefCount() != 0) return true; // I have been referenced
         if(aopInstance.getRefFqn() != null) return true; // I am referencing others

         boolean hasFieldAnnotation = hasAnnotation(obj.getClass(), ((Advised)obj)._getAdvisor(), type);
         // Check the fields
         for (Iterator i = type.getFieldsIterator(); i.hasNext();) {
            Field field = (Field) i.next();
            Object value = null;
            try {
               value=field.get(obj);
            }
            catch(IllegalAccessException e) {
               throw new CacheException("field access failed", e);
            }

            CachedType fieldType = cache_.getCachedType(field.getType());

            // we simply treat field that has @Serializable as a primitive type.
            if (fieldType.isImmediate() ||
                    (hasFieldAnnotation &&
                            CachedType.hasSerializableAnnotation(field, ((Advised)obj)._getAdvisor())))
            {
               continue;
            }

            // check for non-replicatable types
            if(CachedType.isNonReplicatable(field, ((Advised)obj)._getAdvisor()))
            {
               continue;
            }

            if(!hasFieldAnnotation)
            {
               if(CachedType.hasTransientAnnotation(field, ((Advised)obj)._getAdvisor()))
               {
                  continue;
               }
            }

            // Need to do a getObject just in case this is a failover removeObject.
            if(value == null)
              value = _getObject(new Fqn(interceptor.getFqn(), field.getName()));

            if(value == null) continue; // this is no brainer.

            if(pojoGraphMultipleReferenced(value, undoMap)) return true;
         }
         boolean detachOnly = false;
         detachInterceptor(advisor, interceptor, detachOnly, undoMap);
      } else if (obj instanceof Map || obj instanceof List || obj instanceof Set)
      {
         // TODO Is this really necessary?
         if(!(obj instanceof ClassProxy)) return false;

         InstanceAdvisor advisor = ((ClassProxy)obj)._getInstanceAdvisor();
         BaseInterceptor interceptor = (BaseInterceptor)AopUtil.findCollectionInterceptor(advisor);
         AOPInstance aopInstance = interceptor.getAopInstance();
         if(aopInstance == null) return false; // safeguard
         // Check if there is cross referenced.
         if(aopInstance.getRefCount() != 0) return true; // I have been referenced
         if(aopInstance.getRefFqn() != null) return true; // I am referencing others
         // iterate thru the keys
         if(obj instanceof Map)
         {
            for(Iterator it = ((Map)obj).keySet().iterator(); it.hasNext();)
            {
               Object subObj = ((Map)obj).get(it.next());
               if(pojoGraphMultipleReferenced(subObj, undoMap)) return true;
            }
         } else if (obj instanceof List || obj instanceof Set)
         {
            for(Iterator it = ((Collection)obj).iterator(); it.hasNext();)
            {
               Object subObj = it.next();
               if(pojoGraphMultipleReferenced(subObj, undoMap)) return true;
            }
         }
         // Don't remove now.
         boolean removeFromCache = false;
         ((AbstractCollectionInterceptor)interceptor).detach(removeFromCache); // detach the interceptor. This will trigger a copy and remove.
         boolean detachOnly = true;
         detachInterceptor(advisor, interceptor, detachOnly, undoMap);
      }

      return false;
   }

   protected void _regularPutObject(Fqn fqn, Object obj, InstanceAdvisor advisor, CachedType type) throws CacheException
   {
      // TODO workaround for deserialiased objects
      if (advisor == null) {
         advisor = new ClassInstanceAdvisor(obj);
         ((Advised) obj)._setInstanceAdvisor(advisor);
      }

      // Let's do batch update via Map instead
      Map map = new HashMap();
      // Always initialize the ref count so we can mark this as an AopNode.
      AOPInstance aopInstance = internal_.initializeAopInstance(fqn);
      // Insert interceptor at runtime
      CacheInterceptor interceptor = new CacheInterceptor(cache_, fqn, type);
      interceptor.setAopInstance(aopInstance);
      advisor.appendInterceptor(interceptor);
      cache_.addUndoInterceptor(advisor, interceptor, ModificationEntry.INTERCEPTOR_ADD);

      map.put(AOPInstance.KEY, aopInstance);
      // This is put into map first.
      internal_.putAopClazz(fqn, type.getType(), map);
      // we will do it recursively.
      // Map of sub-objects that are non-primitive
      Map subPojoMap = new HashMap();
      boolean hasFieldAnnotation = hasAnnotation(obj.getClass(), ((Advised)obj)._getAdvisor(), type);

      boolean todo = false;
      for (Iterator i = type.getFieldsIterator(); i.hasNext();) {
         Field field = (Field) i.next();
         Object value = null;
         try {
            value=field.get(obj);
         }
         catch(IllegalAccessException e) {
            throw new CacheException("field access failed", e);
         }
         CachedType fieldType = cache_.getCachedType(field.getType());

         if(hasFieldAnnotation)
         {
            if(CachedType.hasTransientAnnotation(field, ((Advised)obj)._getAdvisor()))
            {
               continue;
            }
         }

         // check for non-replicatable types
         if(CachedType.isNonReplicatable(field, ((Advised)obj)._getAdvisor()))
         {
            continue;
         }

         // we simply treat field that has @Serializable as a primitive type.
         if (fieldType.isImmediate() ||
                 (hasFieldAnnotation &&
                         CachedType.hasSerializableAnnotation(field, ((Advised)obj)._getAdvisor())))
         {
            // switched using batch update
            map.put(field.getName(), value);
         } else {
            subPojoMap.put(field, value);
         }
      }

      // Use option to skip locking since we have parent lock already.
      cache_.put(fqn, map, internal_.getLockOption());
      // This is in-memory operation only
      internal_.setPojo(aopInstance, obj);

      Iterator it = subPojoMap.keySet().iterator();
      while(it.hasNext())
      {
         Field field = (Field)it.next();
         Object value= subPojoMap.get(field);
         Fqn tmpFqn = new Fqn(fqn, field.getName());
         _putObject(tmpFqn, value);
         // If it is Collection classes, we replace it with dynamic proxy.
         // But we will have to ignore it if value is null
         collectionHandler_.collectionReplaceWithProxy(obj, value, field, tmpFqn);
      }

      // Need to make sure this is behind put such that obj.toString is done correctly.
      if (log.isDebugEnabled()) {
         log.debug("_regularPutObject(): inserting with fqn: " + fqn);
      }
   }

   private boolean hasAnnotation(Class clazz, Advisor advisor, CachedType type)
   {
      return CachedType.hasAnnotation(clazz, advisor, type);
   }

   private void createNode(Fqn fqn, GlobalTransaction tx) {
      TreeNode n, child_node;
      Object child_name;
      Fqn tmp_fqn=Fqn.ROOT;

      if(fqn == null) return;
         int treeNodeSize=fqn.size();
         n=cache_.getRoot();
         for(int i=0; i < treeNodeSize; i++) {
            child_name=fqn.get(i);
            tmp_fqn=new Fqn(tmp_fqn, child_name);
            child_node=n.getChild(child_name);
            if(child_node == null) {
               child_node=n.createChild(child_name, tmp_fqn, n);
               if(tx != null) {
                  MethodCall undo_op=MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal,
                                                    new Object[]{tx, tmp_fqn, Boolean.FALSE});
                  cache_.addUndoOperation(tx, undo_op);
               }
            }
            n=child_node;
         }
   }

   /**
    * Note that caller of this method will take care of synchronization within the <code>fqn</code> sub-tree.
    *
    * @param fqn
    * @param removeCacheInterceptor
    * @param evict
    * @return
    * @throws CacheException
    */
   public Object _removeObject(Fqn fqn, boolean removeCacheInterceptor, boolean evict)
           throws CacheException
   {
      Class clazz = internal_.peekAopClazz(fqn);
      if (clazz == null)
      {
         if (log.isTraceEnabled()) {
            log.trace("_removeObject(): clasz is null. fqn: " + fqn + " No need to remove.");
         }
         return null;
      }

      if (log.isDebugEnabled()) {
         log.debug("_removeObject(): removing object from fqn: " + fqn);
      }

      Object result = cache_.getObject(fqn);
      if(result == null)
      {
         // This is not a *Pojo*. Must be regular cache stuffs
         if(cache_.exists(fqn))
         {
            // TODO What do we do here. It can still have children pojo though.
            if(!evict)
            {
               cache_.remove(fqn);
            } else
            {
               cache_._evict(fqn);
            }
         }
         return null;
      }

      // can check if we need to do any bulk remove. E.g., if there is no object graph.
      if(getBulkRemove())
      {
         if(bulkRemove(fqn, result))
         {
            // Remember not to print obj here since it will trigger the CacheInterceptor.
            if(log.isDebugEnabled()) {
               log.debug("_removeObject(): fqn: " + fqn + "removing exisiting object in bulk.");
            }

            return result;
         }
         setBulkRemove(false);
      }

      if (graphHandler_.objectGraphRemove(fqn, removeCacheInterceptor, result, evict))
      {
         return result;
      }

      // Not multi-referenced
      if (Advised.class.isAssignableFrom(clazz)) {
         _regularRemoveObject(fqn, removeCacheInterceptor, result, clazz, evict);
      } else if (collectionHandler_.collectionObjectRemove(fqn, removeCacheInterceptor, evict)) {
      } else
      { // Just Serializable objects. Do a brute force remove is ok.
         serializableHandler_.serializableObjectRemove(fqn);
      }

      internal_.cleanUp(fqn, evict);

      // remove the interceptor as well.
      return result;
   }

   protected void _regularRemoveObject(Fqn fqn, boolean removeCacheInterceptor, Object result, Class clazz,
                                       boolean evict) throws CacheException
   {
      Advised advised = ((Advised) result);
      InstanceAdvisor advisor = advised._getInstanceAdvisor();
      Advisor advisorAdvisor = advised._getAdvisor();
      CachedType type = cache_.getCachedType(clazz);
      for (Iterator i = type.getFieldsIterator(); i.hasNext();) {
         Field field = (Field) i.next();

         Object value = null;

         CachedType fieldType = cache_.getCachedType(field.getType());
         if (fieldType.isImmediate()|| CachedType.hasSerializableAnnotation(field, advisorAdvisor)) {
            value = cache_.get(fqn, field.getName());
         } else {
            value = _removeObject(new Fqn(fqn, field.getName()), removeCacheInterceptor, evict);
         }

         // Check for Collection field so we need to restore the original reference.
         if(value instanceof ClassProxy)
         {
            Interceptor interceptor = CollectionInterceptorUtil.getInterceptor((ClassProxy)value);
            value = ((AbstractCollectionInterceptor)interceptor).getOriginalInstance();
         }
         
         try {
            field.set(result, value);
         } catch (IllegalAccessException e) {
            throw new CacheException("field access failed", e);
         }
      }

      // batch remove
      cache_.removeData(fqn);

      // Determine if we want to keep the interceptor for later use.
      if(removeCacheInterceptor) {
         CacheInterceptor interceptor = (CacheInterceptor) AopUtil.findCacheInterceptor(advisor);
         // Remember to remove the interceptor from in-memory object but make sure it belongs to me first.
         if (interceptor != null)
         {
            if (log.isDebugEnabled()) {
               log.debug("regularRemoveObject(): removed cache interceptor fqn: " + fqn + " interceptor: "+interceptor);
            }
            advisor.removeInterceptor(interceptor.getName());
            cache_.addUndoInterceptor(advisor, interceptor, ModificationEntry.INTERCEPTOR_REMOVE);
         }
      }

   }

   boolean isAopNode(Fqn fqn)
   {
      try {
         return (internal_.isAopNode(fqn));
      } catch (Exception e) {
         log.warn("isAopNode(): cache get operation generated exception " +e);
         return false;
      }
   }

   protected Map _findObjects(Fqn fqn) throws CacheException
   {

      // Traverse from fqn to do getObject, if it return a pojo we then stop.
      Map map = new HashMap();
      Object pojo = _getObject(fqn);
      if(pojo != null)
      {
         map.put(fqn, pojo); // we are done!
         return map;
      }

      findChildObjects(fqn, map);
      if(log.isDebugEnabled())
      {
         log.debug("_findObjects(): Fqn: " +fqn + " size of pojos found: " +map.size());
      }
      return map;
   }

   protected void findChildObjects(Fqn fqn, Map map) throws CacheException
   {
      // We need to traverse then
      Set set = cache_.getChildrenNames(fqn);
      if(set == null) return; // We stop here.
      Iterator it = set.iterator();
      while(it.hasNext())
      {
         Object obj = (Object)it.next();
         Fqn newFqn = new Fqn(fqn, obj);

         Object pojo = _getObject(newFqn);
         if(pojo != null)
         {
            map.put(newFqn, pojo);
         } else
         {
            findChildObjects(newFqn, map);
         }
      }
   }
}
