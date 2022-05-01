package org.jboss.cache.rpc;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.jboss.cache.TreeCache;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;

/**
 * {@link TreeCache} extension that adds a general purpose RPC functionality
 * to allow clients to make/receive RPC calls over the same JGroups Channel 
 * used by the cache.
 * <p>
 * Services wishing to receive remote calls should register a unique service 
 * name and an object on which the remote calls for that service can be invoked.
 * </p>
 * <p>
 * Clients wishing to make RPC calls need to know the unique service name, which
 * they can pass to one of the flavors of <code>callRemoteMethods</code>.
 * </p>
 * 
 * <strong>NOTE: </strong> The purpose of this class is to allow services that
 * want to use a TreeCache to avoid also having to use a HAPartition (and thus
 * potentially requiring a duplicate JGroups Channel).
 * 
 * @deprecated This class will be removed when JGroups adds a multiplexing
 *             capability.
 * 
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public class RpcTreeCache extends TreeCache implements RpcTreeCacheMBean
{

   /** The {@link #_dispatchRpcCall(String, MethodCall)} method */
   public static final Method dispatchRpcCallMethod;
   
   static
   {
      try
      {
         dispatchRpcCallMethod=RpcTreeCache.class.getDeclaredMethod("_dispatchRpcCall",
               new Class[]{String.class,
                           MethodCall.class});
      }
      catch(NoSuchMethodException ex) {
         ex.printStackTrace();
         throw new ExceptionInInitializerError(ex.toString());
      }
   }
   
   /** <code>Map</code> of registered RPC handlers */
   protected Map rpcHandlers = new HashMap();
   
   /**
    * Creates a channel with the given properties. Connects to the channel, then creates a PullPushAdapter
    * and starts it
    */
   public RpcTreeCache(String cluster_name, String props, long state_fetch_timeout) throws Exception {
      super(cluster_name, props, state_fetch_timeout);
   }

   /**
    * Default constructor.
    * 
    * @throws Exception
    */
   public RpcTreeCache() throws Exception {
      super();
   }

   /**
    * Expects an already connected channel. Creates a PullPushAdapter and starts it
    */
   public RpcTreeCache(JChannel channel) throws Exception {
      super(channel);
   }

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
   public List callRemoteMethods(String serviceName, Vector members, Method method, Object[] args, boolean synchronous, boolean exclude_self, long timeout) throws Exception
   {
      
      return callRemoteMethods(serviceName, members, MethodCallFactory.create(method, args), synchronous, exclude_self, timeout);
   }

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
   public List callRemoteMethods(String serviceName, Vector mbrs, 
                                 MethodCall method_call, boolean synchronous, 
                                 boolean exclude_self, long timeout) 
         throws Exception
   {
      List responses = null;
      
      if (cache_mode == TreeCache.LOCAL)
      {
         // If cache mode is local, we have no channel
         // so we have to make local calls to our registered handler
         
        if (synchronous)
        {
           // For synchronous calls we have to return something
           responses = new ArrayList();
           
           if (exclude_self == false)
           {
              // Make the call locally and add a valid response to result list
              Object resp = _dispatchRpcCall(serviceName, method_call);
              if ((resp instanceof NoHandlerForRPCException) == false)
                 responses.add(_dispatchRpcCall(serviceName, method_call));
           }  
           // else just return an empty list
           
        }
        else if (exclude_self == false)
        {
           // Asynchronous, so we don't return anything,
           // but we still want to make the local call
           _dispatchRpcCall(serviceName, method_call);
        }
      }
      else {
         // Cache mode is not LOCAL
         // Need to make a call on the cluster
         
         // Wrap the ultimate target in a MethodCall pointing at 
         // the _dispatchRpcCall method
         MethodCall wrapper = MethodCallFactory.create(dispatchRpcCallMethod,
                                             new Object[] { serviceName, method_call});
         
         responses = callRemoteMethods(mbrs, wrapper, synchronous, 
                                       exclude_self, timeout);

         // Remove any NoHandlerForRPCException
         // Its inefficient doing this here, but if we add it to
         // TreeCache.callRemoteMethods we slow down normal cache ops
         if (responses != null)
         {            
            for (int i = 0; i < responses.size(); i++)
            {
               Object obj = responses.get(i);
               if (obj instanceof NoHandlerForRPCException)
               {
                  responses.remove(i);
                  i--;
               }
            }
         }
      }
      
      return responses;
   }

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
   public List callRemoteMethods(String serviceName, Vector members, 
         String method_name, Class[] types, Object[] args, boolean synchronous, 
         boolean exclude_self, long timeout) throws Exception
   {
      Object handler = rpcHandlers.get(serviceName);
      if (handler == null)
      {
         String msg = "No rpc handler registered under: " + serviceName;
         
         log.trace(msg);
         
         throw new NoHandlerForRPCException(msg);
      }
      
      Method method= handler.getClass().getDeclaredMethod(method_name, types);
      return callRemoteMethods(serviceName, members, method, args, 
                               synchronous, exclude_self, timeout);
   }

   
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
   public void registerRPCHandler(String serviceName, Object handler)
   {
      rpcHandlers.put(serviceName, handler);
   }
   
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
   public void unregisterRPCHandler(String serviceName, Object handler)
   {
      Object registered = rpcHandlers.remove(serviceName);
      if (handler != registered)
      {
         // Put it back
         rpcHandlers.put(serviceName, handler);
      }
   }
   
   /**
    * Looks up the RPC handler for <code>serviceName</code> and invokes
    * the method call on it.
    * 
    * @param serviceName   the service
    * @param call          the call to invoke
    * @return  the result of the call, or <code>NoHandlerForRPCException</code>
    *          if no handler was registered for 
    */
   public Object _dispatchRpcCall(String serviceName, MethodCall call)
   {
      Object retval = null;
      Object handler = rpcHandlers.get(serviceName);
      if (handler == null)
      {
         String msg = "No rpc handler registered under: " + serviceName;
         
         log.trace(msg);
         
         return new NoHandlerForRPCException(msg, (Address) getLocalAddress());
      }
      
      try
      {
         retval = call.invoke(handler);
      }
      catch (Throwable t)
      {
         log.trace("rpc call threw exception", t);
         retval = t;
      }
      
      return retval;
   }
   
   

}
