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
import org.jboss.cache.Modification;
import org.jboss.cache.lock.TimeoutException;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;

import java.lang.reflect.Method;
import java.util.*;

/**
 * DelegatingCacheLoader implementation which delegates to a remote (not in the same VM)
 * TreeCache using JGroups' RPC mechanism. The remote TreeCache delegated to is this
 * cacheloader's cache's coordinator.
 * 
 * This CacheLoader uses an optional configuration property, <tt>timeout</tt>, which
 * specifies the timeout in milliseconds for each RPC call. If <tt>timeout</tt> is not
 * specified, it defaults to <tt>5000</tt>.
 * 
 * @author Daniel Gredler
 * @version $Id: RpcDelegatingCacheLoader.java 1927 2006-05-16 22:43:21Z gzamarreno $
 */
public class RpcDelegatingCacheLoader extends DelegatingCacheLoader {

   private int timeout;
   private TreeCache cache;
   private Address localAddress;

   public static final Method METHOD_GET_STATE;
   public static final Method METHOD_SET_STATE;
   public static final Method METHOD_GET_CHILDREN_NAMES;
   public static final Method METHOD_GET_WITH_2_PARAMS;
   public static final Method METHOD_GET_WITH_1_PARAM;
   public static final Method METHOD_EXISTS;
   public static final Method METHOD_PUT_WITH_3_PARAMS;
   public static final Method METHOD_PUT_WITH_2_PARAMS;
   public static final Method METHOD_REMOVE_WITH_2_PARAMS;
   public static final Method METHOD_REMOVE_WITH_1_PARAM;
   public static final Method METHOD_REMOVE_DATA;

   /**
    * Initializes the <tt>Method</tt> instances needed by the operations
    * delegated to the cacheloader's cache's coordinator.
    */
   static {
      try {
         METHOD_GET_STATE = TreeCache.class.getDeclaredMethod("getStateBytes", new Class[] {});
         METHOD_SET_STATE = TreeCache.class.getDeclaredMethod("setStateBytes", new Class[] { byte[].class });
         METHOD_GET_CHILDREN_NAMES = TreeCache.class.getDeclaredMethod("getChildrenNames", new Class[] { Fqn.class });
         METHOD_GET_WITH_2_PARAMS = TreeCache.class.getDeclaredMethod("get", new Class[] { Fqn.class, Object.class });
         METHOD_GET_WITH_1_PARAM = TreeCache.class.getDeclaredMethod("get", new Class[] { Fqn.class });
         METHOD_EXISTS = TreeCache.class.getDeclaredMethod("exists", new Class[] { Fqn.class });
         METHOD_PUT_WITH_3_PARAMS = TreeCache.class.getDeclaredMethod("put", new Class[] { Fqn.class, Object.class, Object.class });
         METHOD_PUT_WITH_2_PARAMS = TreeCache.class.getDeclaredMethod("put", new Class[] { Fqn.class, Map.class });
         METHOD_REMOVE_WITH_2_PARAMS = TreeCache.class.getDeclaredMethod("remove", new Class[] { Fqn.class, Object.class });
         METHOD_REMOVE_WITH_1_PARAM = TreeCache.class.getDeclaredMethod("remove", new Class[] { Fqn.class });
         METHOD_REMOVE_DATA = TreeCache.class.getDeclaredMethod("removeData", new Class[] { Fqn.class });
      } catch (NoSuchMethodException ex) {
         ex.printStackTrace();
         throw new ExceptionInInitializerError(ex.toString());
      }
   }

   /**
    * Default constructor.
    */
   public RpcDelegatingCacheLoader() {
      // Empty.
   }

   /**
    * Allows programmatic configuration.
    * 
    * @param timeout The timeout in milliseconds for each RPC call.
    */
   public RpcDelegatingCacheLoader(TreeCache cache, int timeout) {
      this.cache = cache;
      this.timeout = timeout;
   }

   /**
    * Allows configuration via XML config file.
    * 
    * @see org.jboss.cache.loader.DelegatingCacheLoader#setConfig(java.util.Properties)
    */
   public void setConfig(Properties props) {
      if(props == null) return;
      String t = props.getProperty("timeout");
      this.timeout = (t == null || t.length() == 0 ? 5000 : Integer.parseInt(t));
   }

   /**
    * Allows configuration via XML config file.
    * 
    * @see org.jboss.cache.loader.DelegatingCacheLoader#setCache(org.jboss.cache.TreeCache)
    */
   public void setCache(TreeCache cache) {
      this.cache = cache;
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGetChildrenNames(org.jboss.cache.Fqn)
    */
   protected Set delegateGetChildrenNames(Fqn name) throws Exception {
      return (Set) this.doMethodCall( METHOD_GET_CHILDREN_NAMES, new Object[] { name } );
   }

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.
   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGet(org.jboss.cache.Fqn, java.lang.Object)
    */
//   protected Object delegateGet(Fqn name, Object key) throws Exception {
//      return this.doMethodCall( METHOD_GET_WITH_2_PARAMS, new Object[] { name, key } );
//   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGet(org.jboss.cache.Fqn)
    */
   protected Map delegateGet(Fqn name) throws Exception {
      DataNode n=(DataNode)this.doMethodCall( METHOD_GET_WITH_1_PARAM, new Object[] { name } );
      if(n == null)
         return null;
      return n.getData();
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateExists(org.jboss.cache.Fqn)
    */
   protected boolean delegateExists(Fqn name) throws Exception {
      Boolean exists = (Boolean) this.doMethodCall( METHOD_EXISTS, new Object[] { name } );
      return exists != null && exists.booleanValue();
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegatePut(org.jboss.cache.Fqn, java.lang.Object, java.lang.Object)
    */
   protected Object delegatePut(Fqn name, Object key, Object value) throws Exception {
      return this.doMethodCall( METHOD_PUT_WITH_3_PARAMS, new Object[] { name, key, value } );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegatePut(org.jboss.cache.Fqn, java.util.Map)
    */
   protected void delegatePut(Fqn name, Map attributes) throws Exception {
      this.doMethodCall( METHOD_PUT_WITH_2_PARAMS, new Object[] { name, attributes } );
   }

   // todo: we should probably implement put(List) in TreeCache itself, so we don't need to invoke each mod separately
   protected void delegatePut(List modifications) throws Exception {
      for(Iterator it=modifications.iterator(); it.hasNext();) {
         Modification m=(Modification)it.next();
         switch(m.getType()) {
            case Modification.PUT_DATA:
               put(m.getFqn(), m.getData());
               break;
            case Modification.PUT_DATA_ERASE:
               put(m.getFqn(), m.getData(), true);
               break;
            case Modification.PUT_KEY_VALUE:
               put(m.getFqn(), m.getKey(), m.getValue());
               break;
            case Modification.REMOVE_DATA:
               removeData(m.getFqn());
               break;
            case Modification.REMOVE_KEY_VALUE:
               remove(m.getFqn(), m.getKey());
               break;
            case Modification.REMOVE_NODE:
               remove(m.getFqn());
               break;
            default:
               log.error("modification type " + m.getType() + " not known");
               break;
         }
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemove(org.jboss.cache.Fqn, java.lang.Object)
    */
   protected Object delegateRemove(Fqn name, Object key) throws Exception {
      return this.doMethodCall( METHOD_REMOVE_WITH_2_PARAMS, new Object[] { name, key } );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemove(org.jboss.cache.Fqn)
    */
   protected void delegateRemove(Fqn name) throws Exception {
      this.doMethodCall( METHOD_REMOVE_WITH_1_PARAM, new Object[] { name } );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemoveData(org.jboss.cache.Fqn)
    */
   protected void delegateRemoveData(Fqn name) throws Exception {
      this.doMethodCall( METHOD_REMOVE_DATA, new Object[] { name } );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateLoadEntireState()
    */
   public byte[] delegateLoadEntireState() throws Exception {
      return (byte[]) this.doMethodCall( METHOD_GET_STATE, new Object[0] );
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateStoreEntireState(byte[])
    */
   public void delegateStoreEntireState(byte[] state) throws Exception {
      this.doMethodCall( METHOD_SET_STATE, new Object[] { state } );
   }

   /**
    * Performs the specified remote method call using the specified arguments. This method
    * returns <tt>null</tt> if unable to delegate the method call to the cacheloader's
    * cache's coordinator because it is in fact itself the coordinator.
    * 
    * @param method The remote method to call.
    * @param args The arguments to use for the remote method call.
    * @return The value returned by the remote method call.
    */
   private Object doMethodCall( Method method, Object[] args ) throws Exception {
      if( this.cache.isCoordinator() ) {
         if( log.isTraceEnabled() ) {
            log.trace( "Cannot delegate to the remote coordinator because the cache is itself the coordinator." );
         }
         return null;
      }
      if( this.localAddress == null ) {
         this.localAddress = (Address) this.cache.getLocalAddress();
      }
      if( this.localAddress == null ) {
         throw new Exception( "Cannot delegate to the remote coordinator because the cache has no local address." );
      }
      Address coordinator = cache.getCoordinator();
      if( coordinator == null ) {
         throw new Exception( "Cannot delegate to the remote coordinator because the cache has no coordinator." );
      }
      Vector members = new Vector();
      members.add( coordinator );
      MethodCall methodCall = MethodCallFactory.create( method, args );
      boolean synchronous = true;
      boolean excludeSelf = true;
      List responses = cache.callRemoteMethods( members, methodCall, synchronous, excludeSelf, this.timeout );
      if( responses == null ) {
         throw new Exception( "Remote method call [" + cache.getLocalAddress() + "]->[" + coordinator + "]." + methodCall.getMethod().getName() + "() was discarded!" );
      }
      Object response = responses.get( 0 );
      if( response instanceof TimeoutException ) {
         throw new Exception( "Remote method call [" + cache.getLocalAddress() + "]->[" + coordinator + "]." + methodCall.getMethod().getName() + "() timed out: " + response );
      }
      else if( response instanceof Throwable ) {
         throw new Exception( "Remote method call [" + cache.getLocalAddress() + "]->[" + coordinator + "]." + methodCall.getMethod().getName() + "() failed!", (Throwable) response );
      }
      return response;
   }

}
