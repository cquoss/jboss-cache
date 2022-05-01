/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.rpc;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Vector;

import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeCacheMBean;
import org.jgroups.blocks.MethodCall;

/**
 * MBean interface for the {@link RpcTreeCache}.
 *  
 * @deprecated This class will be removed when JGroups adds a multiplexing
 *             capability.
 *             
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public interface RpcTreeCacheMBean extends TreeCacheMBean
{

   /**
    * Calls a remote method on nodes in the cluster, targeted at 
    * objects registered under a given <code>serviceName</code>.
    * 
    * 
    * @param serviceName   name of a callback handler that will have been
    *                      registered on the remote end via 
    *                      {@link #registerRPCHandler(String, Object)}.
    * @param members       Vector, each of whose members is the Address of one
    *                      the nodes in the cache's 
    *                      {@link TreeCache#getMembers() member list}.
    *                      If <code>null</code>, the method will be invoked on
    *                      all members.
    * @param method        method to execute
    * @param args          method arguments
    * @param synchronous   <code>true</code> if the call should block until
    *                      all members respond (or timeout); <code>false</code>
    *                      if the call should return immediately without 
    *                      waiting for responses
    * @param exclude_self  should the call be invoked on the callee?
    * @param timeout       how long to wait for synchronous responses
    * @return              List containing the responses that were received, or 
    *                      <code>null</code> if the call is asynchronous.
    *                      Elements of the list will either be a returned value
    *                      or an exception if one was returned.  Any
    *                      NoHandlerForRPCException returned will be removed.
    * 
    * @throws Exception
    */
   public abstract List callRemoteMethods(String serviceName, Vector members, Method method, Object[] args,
         boolean synchronous, boolean exclude_self, long timeout) throws Exception;

   /**
    * Calls a remote method on nodes in the cluster, targeted at 
    * objects registered under a given <code>serviceName</code>.
    * <p>
    * If the cache's <code>cache mode</code> is <code>TreeCache.LOCAL</code>
    * and parameter <code>exclude_self</code> is <code>false</code>
    * this request will be made directly to {@link #_dispatchRpcCall()}.
    * </p>
    * 
    * @param serviceName   name of a callback handler that will have been
    *                      registered on the remote end via 
    *                      {@link #registerRPCHandler(String, Object)}.
    * @param members       Vector, each of whose members is the Address of one
    *                      the nodes in the cache's 
    *                      {@link TreeCache#getMembers() member list}.
    *                      If <code>null</code>, the method will be invoked on
    *                      all members.
    * @param method_call   method call to execute
    * @param synchronous   <code>true</code> if the call should block until
    *                      all members respond (or timeout); <code>false</code>
    *                      if the call should return immediately without 
    *                      waiting for responses
    * @param exclude_self  should the call be invoked on the callee?
    * @param timeout       how long to wait for synchronous responses
    * @return              List containing the responses that were received, or 
    *                      <code>null</code> if the call is asynchronous.
    *                      Elements of the list will either be a returned value
    *                      or an exception if one was returned.  Any
    *                      NoHandlerForRPCException returned will be removed.
    * 
    * @throws Exception
    */
   public abstract List callRemoteMethods(String serviceName, Vector mbrs, MethodCall method_call, boolean synchronous,
         boolean exclude_self, long timeout) throws Exception;

   /**
    * Calls a remote method on nodes in the cluster, targeted at 
    * objects registered under a given <code>serviceName</code>.
    * 
    * 
    * @param serviceName   name of a callback handler that will have been
    *                      registered on the remote end via 
    *                      {@link #registerRPCHandler(String, Object)}.
    * @param members       Vector, each of whose members is the Address of one
    *                      the nodes in the cache's 
    *                      {@link TreeCache#getMembers() member list}.
    *                      If <code>null</code>, the method will be invoked on
    *                      all members.
    * @param method_name   name of the method to execute
    * @param args          method arguments
    * @param synchronous   <code>true</code> if the call should block until
    *                      all members respond (or timeout); <code>false</code>
    *                      if the call should return immediately without 
    *                      waiting for responses
    * @param exclude_self  should the call be invoked on the callee?
    * @param timeout       how long to wait for synchronous responses
    * @return              List containing the responses that were received, or 
    *                      <code>null</code> if the call is asynchronous.
    *                      Elements of the list will either be a returned value
    *                      or an exception if one was returned.  Any
    *                      NoHandlerForRPCException returned will be removed.
    *                     
    * @throws NoHandlerForRPCException if no handler is registered on this node
    *                                  under <code>serviceName</code>.
    * 
    * @throws Exception
    */
   public abstract List callRemoteMethods(String serviceName, Vector members, String method_name, Class[] types,
         Object[] args, boolean synchronous, boolean exclude_self, long timeout) throws Exception;

   /**
    * Registers the given object as the on which any MethodCall associated with
    * the given service should be invoked.
    * 
    * @param serviceName   name of a service that will be receiving RPC calls
    * @param handler       object on which RPC calls for 
    *                      <code>serviceName</code> can be invoked.
    *                      
    * @see #_dispatchRpcCall
    */
   public abstract void registerRPCHandler(String serviceName, Object handler);

   /**
    * Removes the given object as a handler for RPC calls for the given
    * service.
    * 
    * @param serviceName   name of a service that will be receiving RPC calls
    * @param handler       object that was previously passed to
    *                      {@link #registerRPCHandler(String, Object)} for 
    *                      <code>serviceName</code>.
    * @param handler
    */
   public abstract void unregisterRPCHandler(String serviceName, Object handler);

}