package org.jboss.cache.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.AbstractTreeCacheListener;
import org.jgroups.Address;
import org.jgroups.View;

import java.util.*;

/**
 * CacheLoader proxy used only when multiple CacheLoaders in a cluster access the same underlying store (e.g.
 * a shared filesystem, or DB). SharedStoreCacheLoader is a simply facade to a real CacheLoader implementation. It
 * always delegates reads to the real CacheLoader. Writes are forwarded only if this SharedStoreCacheLoader is
 * currently the cordinator. This avoid having all CacheLoaders in a cluster writing the same data to the same
 * underlying store. Although not incorrect (e.g. a DB will just discard additional INSERTs for the same key, and
 * throw an exception), this will avoid a lot of redundant work.<br/>
 * Whenever the current coordinator dies (or leaves), the second in line will take over. That SharedStoreCacheLoader
 * will then pass writes through to its underlying CacheLoader.
 * @author Bela Ban
 * @version $Id: SharedStoreCacheLoader.java 1523 2006-04-08 20:30:08Z genman $
 */
public class SharedStoreCacheLoader extends AbstractTreeCacheListener implements CacheLoader {

   private Log         log=LogFactory.getLog(getClass());
   private CacheLoader loader=null;
   private Address     local_addr=null;
   private boolean     active=true; // only active if coordinator
   private TreeCache   cache=null;

   public SharedStoreCacheLoader(CacheLoader loader, Address local_addr, boolean coordinator) {
      this.loader=loader;
      this.local_addr=local_addr;
      this.active=coordinator;
   }

   public void nodeCreated(Fqn fqn) {}
   public void nodeRemoved(Fqn fqn) {}
   public void nodeLoaded(Fqn fqn) {}
   public void nodeEvicted(Fqn fqn) {}
   public void nodeModified(Fqn fqn) {}
   public void nodeVisited(Fqn fqn) {}
   public void cacheStarted(TreeCache cache) {}
   public void cacheStopped(TreeCache cache) {}

   public void viewChange(View new_view) {
      boolean tmp=active;
      if(new_view != null && local_addr != null) {
         Vector mbrs=new_view.getMembers();
         if(mbrs != null && mbrs.size() > 0 && local_addr.equals(mbrs.firstElement())) {
            tmp=true;
         }
         else {
            tmp=false;
         }
      }
      if(active != tmp) {
         active=tmp;
         log.info("changed mode: active=" + active);
      }
   }


   public void setConfig(Properties props) {
      loader.setConfig(props);
   }

   public void setCache(TreeCache c) {
      this.cache=c;
      loader.setCache(c);
   }

   public Set getChildrenNames(Fqn fqn) throws Exception {
      return loader.getChildrenNames(fqn);
   }
// See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.
//   public Object get(Fqn name, Object key) throws Exception {
//      return loader.get(name, key);
//   }

   public Map get(Fqn name) throws Exception {
      return loader.get(name);
   }

   public boolean exists(Fqn name) throws Exception {
      return loader.exists(name);
   }

   public Object put(Fqn name, Object key, Object value) throws Exception {
      if(active)
         return loader.put(name, key, value);
      else
         return null;
   }

   public void put(Fqn name, Map attributes) throws Exception {
      if(active)
         loader.put(name, attributes);
   }

   public void put(List modifications) throws Exception {
      if(active)
         loader.put(modifications);
   }

   public Object remove(Fqn name, Object key) throws Exception {
      if(active)
         return loader.remove(name, key);
      return null;
   }

   public void remove(Fqn name) throws Exception {
      if(active)
         loader.remove(name);
   }

   public void removeData(Fqn name) throws Exception {
      if(active)
         loader.removeData(name);
   }

   public void prepare(Object tx, List modifications, boolean one_phase) throws Exception {
      if(active)
         loader.prepare(tx, modifications, one_phase);
   }

   public void commit(Object tx) throws Exception {
      if(active)
         loader.commit(tx);
   }

   public void rollback(Object tx) {
      if(active)
         loader.rollback(tx);
   }

   public byte[] loadEntireState() throws Exception {
      return loader.loadEntireState();
   }

   public void storeEntireState(byte[] state) throws Exception {
      if(active)
         loader.storeEntireState(state);
   }

   public void create() throws Exception {
      loader.create();
   }

   public void start() throws Exception {
      loader.start();
   }

   public void stop() {
      loader.stop();
   }

   public void destroy() {
      loader.destroy();
   }

}
