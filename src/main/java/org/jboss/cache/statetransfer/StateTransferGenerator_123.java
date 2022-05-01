/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.DataNode;
import org.jboss.cache.TreeCache;
import org.jgroups.util.Util;

class StateTransferGenerator_123 implements StateTransferGenerator
{
   private Log log = LogFactory.getLog(getClass().getName());

   private TreeCache cache;

   StateTransferGenerator_123(TreeCache cache)
   {
      this.cache = cache;
   }
   
   public byte[] generateStateTransfer(DataNode rootNode, 
                                       boolean generateTransient, 
                                       boolean generatePersistent,
                                       boolean suppressErrors) 
      throws Throwable
   {
      if (rootNode.getFqn().size() > 0)
      {
         throw new IllegalArgumentException("Invalid node " + rootNode.getFqn() + 
                                            " -- StateTransferVersion 123 only supports " +
                                            "transferring  FQN '/'");  
      }
      
      byte[] transient_state=null;
      byte[] persistent_state=null;
      byte[][] states=new byte[2][];
      byte[] retval=null;
      
      states[0]=states[1]=null;
      try {
         if(generateTransient) {
            log.info("locking the tree to obtain transient state");
            transient_state=Util.objectToByteBuffer(rootNode);
            states[0]=transient_state;
            log.info("returning the transient state (" + transient_state.length + " bytes)");
         }
      }
      catch(Throwable t) {
         log.error("failed getting the transient state", t);
      }
      try {
         if(generatePersistent) {
            log.info("getting the persistent state");
            persistent_state=cache.getCacheLoader().loadEntireState();
            states[1]=persistent_state;
            log.info("returning the persistent state (" + persistent_state.length + " bytes)");
         }
      }
      catch(Throwable t) {
         log.error("failed getting the persistent state", t);
      }

      try {
         retval=Util.objectToByteBuffer(states);
      }
      catch(Throwable t) {
         log.error("failed serializing transient and persistent state", t);
      }
      
      return retval;
   }

}
