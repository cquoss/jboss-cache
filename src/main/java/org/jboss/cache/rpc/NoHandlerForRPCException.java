package org.jboss.cache.rpc;

import org.jgroups.Address;

/**
 * Exception returned when 
 * {@link RpcTreeCache#_dispatchRpcCall(String, org.jgroups.blocks.MethodCall)} 
 * is passed a call for an unregistered handler.
 * 
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public class NoHandlerForRPCException extends Exception
{
   /** The serialVersionUID */
   private static final long serialVersionUID = 1L;

   private Address nodeAddress;
   
   public NoHandlerForRPCException(String msg)
   {
      super(msg);
   }

   public NoHandlerForRPCException(String message, Address failedNode)
   {
      super(message);
      this.nodeAddress = failedNode;
   }

   public Address getNodeAddress()
   {
      return nodeAddress;
   }

   public void setNodeAddress(Address failedNode)
   {
      this.nodeAddress = failedNode;
   }
   
   

}
