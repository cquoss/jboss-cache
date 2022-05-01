/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jboss.cache.TreeCache;
import org.jboss.cache.Fqn;
import org.jboss.cache.CacheException;
import org.jboss.cache.TreeCacheListener;
import org.jboss.cache.lock.IsolationLevel;
import org.jboss.invocation.MarshalledValue;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.stack.IpAddress;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;

/**
 * <p>Version of TreeCache that added call to handle marshalling values. You will need marshalling when your application
 * is running under different class loader scope, for example, under JBoss AS where your application has a scoped
 * class loader.</p>
 * <p>Note that: Currently, we also have a in-memory cache copy to minimize the call to unmarshalling. And we also
 * have an invalidation mechanism in place to synchronize the external updates.</p>
 * <p>In the future, it'd be best if JBossCache can provides 1) notification excluding myself, 2) notification granulairty
 * with specific modified key, 3) we will also move this to different package.</p>
 * <p>Finally, since the use of in-memory copy, the memory usage is almost doubled since we have one in-memory copy and
 * the marshalled value in the cache store.</p>
 * @author Ben Wang
 */
public class MarshalledTreeCache extends TreeCache implements TreeCacheListener {
   // Store the in-memory copy of the treecache (key, value) pair.
   // This is used for performance reason so there is no need to un-marshall every single operation.
   // In addition, it will support an invalidation mechanism.
   protected TreeCache localCopy_;
   protected String nodeId_;
   // Key to denotes the caller's ID. We will use this to check whether this is from myself or not.
   // TODO Will need to document this.
   protected static final String NODEID_KEY = "__NODEID_KEY__";
   // Context class loader. If it is not null, marshalling/unmarshalling will use this.
   protected ClassLoader tcl_ = null;
   // If it is on, will use an internal copy to keep the unmarshalling value.
   protected boolean useLocalOptimization_ = true;
   // Indicate whether we want marshalling or not. If it is false, useLocalOptimization will be false as wel.
   protected boolean marshalling_ = true;

   public MarshalledTreeCache(String cluster_name,
                       String props,
                       long state_fetch_timeout)
         throws Exception
   {
      super(cluster_name, props, state_fetch_timeout);
      this._init();
   }

   public MarshalledTreeCache() throws Exception
   {
      this._init();
   }

   public MarshalledTreeCache(JChannel channel) throws Exception
   {
      super(channel);
      this._init();
   }

   private void _init() throws Exception {
      localCopy_ = new TreeCache();
      localCopy_.setCacheMode(TreeCache.LOCAL);
      localCopy_.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      marshalling_ = true;
      useLocalOptimization_ = true;
      tcl_ = null;
   }

   public void startService() throws Exception
   {
      super.addTreeCacheListener(this);
      super.startService();
      if(localCopy_ == null)
         throw new RuntimeException("startService(): null localCopy_");
      localCopy_.startService();
      obtainNodeId();
   }

   public void stopService()
   {
      nodeId_ = null;
      localCopy_.stopService();
      super.stopService();
   }

   /**
    * Get a node id based on jgroups properties.
    */
   protected void obtainNodeId()
   {
      IpAddress address = (IpAddress)getLocalAddress();
      if(address == null)
      {
         log.info("obtainNodeId(): has null IpAddress. Assume it is running in local mode.");
         nodeId_ = "local";
         return;
      }

      if (address.getAdditionalData() == null)
      {
         nodeId_ = address.getIpAddress().getHostAddress() + ":" + address.getPort();
      }
      else
      {
         nodeId_ = new String(address.getAdditionalData());
      }
   }

   /**
    * DataNode id is a communication id that denotes the cluster node.
    */
   public String getNodeId() {
      return nodeId_;
   }


   /**
    * Turn marshalling layer on or off. If off, no marshalling. Default is on.
    *
    */
   public void setMarshalling(boolean marshalling)
   {
      marshalling_ = marshalling;
   }

   /**
    * Indicate whether to have a in-memory copy of the unmarshalling object such that
    * there is no need to unmarshal. If it is on, invlidation will be handled where another active
    * node has update this fqn.
    */
   public void setLocalOptimization(boolean optimization)
   {
      useLocalOptimization_ = optimization;
      throw new RuntimeException("MarshalledTreeCache.setLocalOptimization(): operation not supported yet.");
   }

   /**
    * The context class loader to perform marshalling/unmarshalling
    */
   public void setClassLoader(ClassLoader tcl)
   {
      tcl_ = tcl;
   }

   public void marshalledPut(String fqn, Object key, Object value) throws CacheException {
      marshalledPut(Fqn.fromString(fqn), key, value);
   }

   /**
    * Marshalled put. That is, the value that is put into cache is marshalled first. Note that
    * we still require that key to be primitive type.
    */
   public void marshalledPut(Fqn fqn, Object key, Object value) throws CacheException
   {
      if(marshalling_)
      {
         marshalledPut_(fqn, key, value);
      } else {
         put(fqn, key, value);
      }
   }

   public void marshalledPut_(Fqn fqn, Object key, Object value) throws CacheException
   {
      MarshalledValue mv = null;
      try {
         mv = new MarshalledValue(value);
      } catch (IOException e) {
         e.printStackTrace();
         throw new CacheException("marshalledPut() exception: " +e);
      }

      // Put into local copy first.
      localCopy_.put(fqn, key, value);
      // Put into cache
      Map map = new HashMap();
      map.put(key, mv);
      map.put(NODEID_KEY, nodeId_);
      this.put(fqn, map);
   }

   public Object marshalledGet(String fqn, Object key) throws CacheException {
      return marshalledGet(Fqn.fromString(fqn), key);
   }

   /**
    * Obtain the value from the marshalled cache. Note that the return value is un-marshalled
    * either from the local copy or from the distributed store.
    */
   public Object marshalledGet(Fqn fqn, Object key) throws CacheException {
      if(marshalling_)
      {
         ClassLoader prevTCL = null;
         if(tcl_ != null)
         {
            prevTCL = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(tcl_);
         }
         try {
            return marshalledGet_(fqn, key);
         } finally
         {
            if(tcl_ != null && prevTCL != null)
            {
               Thread.currentThread().setContextClassLoader(prevTCL);
            }
         }
      } else
      {
         return get(fqn, key);
      }
   }

   public Object marshalledGet_(Fqn fqn, Object key) throws CacheException {
      // Check if it is in local copy first.
      Object value;
      try {
         if( (value = localCopy_.get(fqn, key)) != null)
            return value;
         else
         {  // get it from cache store
            value = get(fqn, key);
            if(value == null) return null;
            checkValue(value);
            MarshalledValue mv = (MarshalledValue)value;
            value = mv.get();
            // populate the local copy
            localCopy_.put(fqn, key, value);
            return value;
         }
      } catch (IOException e) {
         e.printStackTrace();
         throw new CacheException("marshalledGet(): exception encountered: ", e);
      } catch (ClassNotFoundException e) {
         e.printStackTrace();
         throw new CacheException("marshalledGet(): exception encountered: ", e);
      }
   }

   public Object marshalledRemove(String fqn, Object key) throws CacheException
   {
      return marshalledRemove(Fqn.fromString(fqn), key);
   }

   /**
    * Remove a marshalled node. This is required if you have performed a marshalledPut since
    * we will need to do clean up.
    */
   public Object marshalledRemove(Fqn fqn, Object key) throws CacheException
   {
      if(marshalling_)
      {
         return marshalledRemove_(fqn, key);
      } else
      {
         return remove(fqn, key);
      }
   }

   public Object marshalledRemove_(Fqn fqn, Object key) throws CacheException
      {
      if( !exists(fqn, key) )
         log.warn("marshalledRemove(): fqn: " +fqn + " key: " +key + " not found.");

      Object value = localCopy_.get(fqn, key);
      localCopy_.remove(fqn);
      remove(fqn, NODEID_KEY);
      Object obj = remove(fqn, key);
      if(value != null) return value;
      checkValue(obj);
      try {
         return ((MarshalledValue)obj).get();
      } catch (IOException e) {
         e.printStackTrace();
         throw new CacheException("marshalledRemove(): exception encountered: ", e);
      } catch (ClassNotFoundException e) {
         e.printStackTrace();
         throw new CacheException("marshalledRemove(): exception encountered: ", e);
      }
   }

   public void nodeCreated(Fqn fqn)
   {
      // no-op
   }

   public void nodeRemoved(Fqn fqn)
   {
      invalidate(fqn);
   }

   public void nodeLoaded(Fqn fqn)
   {
      // no-op
   }
   
   public void nodeEvicted(Fqn fqn)
   {
      invalidate(fqn);
   }

   public void nodeModified(Fqn fqn)
   {
      invalidate(fqn);
   }

   public void nodeVisited(Fqn fqn)
   {
      // no-op
   }

   public void cacheStarted(TreeCache cache)
   {
      // no-op
   }

   public void cacheStopped(TreeCache cache)
   {
      // no-op
   }

   public void viewChange(View new_view)
   {
      // no-op
   }

   protected void checkValue(Object value)
   {
      if( value != null && !(value instanceof MarshalledValue))
         throw new RuntimeException("checkValue: return object is not instance of MarshalledValue. object: "+value);
   }

   /**
    * Invalidate the local copy cache. Assumption is invlidation should not happen that often anyway.
    * In addition, we will invalidate the whole thing under the fqn.
    * @param fqn
    */
   protected void invalidate(Fqn fqn)
   {
      if(!marshalling_) return; // No need if there is no marshalling!
      if(fqn.isRoot()) return; // No need to handle root.
      if( !localCopy_.exists(fqn)) return; // probably not a mv node anyway.

      try {
         String eventId = (String)get(fqn, NODEID_KEY);
         if(eventId == null)
            throw new RuntimeException("invlidate(): fqn to invlidate has null node id. fqn: " +fqn);

         if( nodeId_.equals(eventId) ) return; // skip since this event is initiated by myself.
         localCopy_.remove(fqn);
      } catch (CacheException e) {
         e.printStackTrace();
      }
   }
}
