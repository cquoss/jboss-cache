/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader;

import org.jboss.cache.Fqn;
import org.jboss.cache.DataNode;
import org.jboss.cache.TreeCache;
import org.jboss.cache.loader.rmi.RemoteTreeCache;

import java.rmi.Naming;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.List;

/**
 * DelegatingCacheLoader implementation which delegates to a remote (not in the same VM)
 * TreeCache using the Java RMI mechanism. If configured via an XML configuration file,
 * the remote TreeCache delegated to is this cacheloader's cache's coordinator. If
 * configured programmatically, this cacheloader may delegate to any remote cache that
 * has been appropriately bound.
 * 
 * This CacheLoader expects three configuration properties: <tt>host</tt>, <tt>port</tt>
 * and <tt>bindName</tt>. If the <tt>host</tt> propety is not specified, it defaults to
 * <tt>localhost</tt>; if the <tt>port</tt> property is not specified, it defaults to
 * <tt>1098</tt>; if the <tt>bindName</tt> property is not specified, it defaults to the
 * cacheloader's cache's cluster name.
 * 
 * @author Daniel Gredler
 * @version $Id: RmiDelegatingCacheLoader.java 2657 2006-10-09 15:40:37Z msurtani $
 */
public class RmiDelegatingCacheLoader extends DelegatingCacheLoader {

   private String host;
   private String port;
   private String bindName;
   private TreeCache cache;
   private RemoteTreeCache remoteCache;
   private boolean programmaticInit;

   /**
    * Default constructor.
    */
   public RmiDelegatingCacheLoader() {
      // Empty.
   }

   /**
    * Allows programmatic configuration.
    * 
    * @param cache The cache that the cacheloader will acting on behalf of.
    * @param host The host on which to look up the remote object.
    * @param port The port on which to look up the remote object.
    * @param bindName The name to which the remote object is bound.
    */
   public RmiDelegatingCacheLoader(TreeCache cache, String host, int port, String bindName) {
      this.cache = cache;
      this.host = host;
      this.port = String.valueOf(port);
      this.bindName = bindName; 
      this.tryToInitRemoteCache();
      this.programmaticInit = true;
   }

   /**
    * Allows configuration via XML config file.
    * 
    * @see org.jboss.cache.loader.DelegatingCacheLoader#setConfig(java.util.Properties)
    */
   public void setConfig(Properties props) {
      this.host = props.getProperty("host");
      if(this.host == null || this.host.length() == 0) {
         this.host = "localhost";
      }
      this.port = props.getProperty("port");
      if(this.port == null || this.port.length() == 0) {
         this.port = "1098";
      }
      this.bindName = props.getProperty("bindName");
      this.tryToInitRemoteCache();
   }

   /**
    * Allows configuration via XML config file.
    * 
    * @see org.jboss.cache.loader.DelegatingCacheLoader#setCache(org.jboss.cache.TreeCache)
    */
   public void setCache(TreeCache cache) {
      this.cache = cache;
      this.tryToInitRemoteCache();
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGetChildrenNames(org.jboss.cache.Fqn)
    */
   protected Set delegateGetChildrenNames(Fqn fqn) throws Exception {
      return ( this.remoteCache != null ? this.remoteCache.getChildrenNames(fqn) : null );
   }

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.
   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGet(org.jboss.cache.Fqn, java.lang.Object)
    */
//   protected Object delegateGet(Fqn name, Object key) throws Exception {
//      return ( this.remoteCache != null ? this.remoteCache.get(name, key) : null );
//   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGet(org.jboss.cache.Fqn)
    */
   protected Map delegateGet(Fqn name) throws Exception {
      DataNode n=this.remoteCache != null ? this.remoteCache.get(name) : null;
      if(n == null)
         return null;
       return n.getData();
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateExists(org.jboss.cache.Fqn)
    */
   protected boolean delegateExists(Fqn name) throws Exception {
      return (this.remoteCache != null && this.remoteCache.exists(name) );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegatePut(org.jboss.cache.Fqn, java.lang.Object, java.lang.Object)
    */
   protected Object delegatePut(Fqn name, Object key, Object value) throws Exception {
      return ( this.remoteCache != null ? this.remoteCache.put(name, key, value) : null );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegatePut(org.jboss.cache.Fqn, java.util.Map)
    */
   protected void delegatePut(Fqn name, Map attributes) throws Exception {
      if( this.remoteCache != null ) this.remoteCache.put(name, attributes);
   }

   protected void delegatePut(List modifications) throws Exception {

   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemove(org.jboss.cache.Fqn, java.lang.Object)
    */
   protected Object delegateRemove(Fqn name, Object key) throws Exception {
      return ( this.remoteCache != null ? this.remoteCache.remove(name, key) : null );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemove(org.jboss.cache.Fqn)
    */
   protected void delegateRemove(Fqn name) throws Exception {
      if( this.remoteCache != null ) this.remoteCache.remove(name);
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemoveData(org.jboss.cache.Fqn)
    */
   protected void delegateRemoveData(Fqn name) throws Exception {
      if( this.remoteCache != null ) this.remoteCache.removeData(name);
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateLoadEntireState()
    */
   public byte[] delegateLoadEntireState() throws Exception {
       throw new UnsupportedOperationException("operation is not currently supported - need to define semantics first");   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateStoreEntireState(byte[])
    */
   public void delegateStoreEntireState(byte[] state) throws Exception {
       throw new UnsupportedOperationException("operation is not currently supported - need to define semantics first");   }

   /**
    * Tries to initialize the remote cache object. If this cacheloader has been
    * cofigured using an XML file
    */
   private void tryToInitRemoteCache() {
      if(this.host == null || this.port == null || this.cache == null) {
         return;
      }
      if(this.bindName == null) {
         this.bindName = this.cache.getClusterName();
      }
      if(!this.programmaticInit && this.cache.isCoordinator()) {
         // CacheLoader specified via XML, but this cache is the coordinator!
         this.remoteCache = null;
         return;
      }
      String name = "//" + this.host + ":" + this.port + "/" + this.bindName;
      try {
         this.remoteCache = (RemoteTreeCache) Naming.lookup(name);
      }
      catch(Throwable t) {
         log.error("Unable to lookup remote cache at '" + name + "'.", t);
      }
   }

}
