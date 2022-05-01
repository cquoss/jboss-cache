/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.aop.InternalDelegate;
import org.jboss.cache.aop.PojoCache;
import org.jboss.cache.aop.util.ObjectUtil;
import org.jboss.cache.loader.ExtendedCacheLoader;
import org.jboss.cache.loader.NodeData;
import org.jboss.invocation.MarshalledValueOutputStream;

class StateTransferGenerator_124 implements StateTransferGenerator
{
   public static final short STATE_TRANSFER_VERSION = 124;
   
   private Log log = LogFactory.getLog(getClass().getName());
   
   private TreeCache cache;

   StateTransferGenerator_124(TreeCache cache)
   {
      this.cache = cache;
   }
   
   public byte[] generateStateTransfer(DataNode rootNode,
                                       boolean generateTransient,
                                       boolean generatePersistent,
                                       boolean suppressErrors)
         throws Throwable
   {      
      boolean debug = log.isDebugEnabled();
      
      Fqn fqn = rootNode.getFqn();
      
      byte[][] states=new byte[3][]; // [transient][associated][persistent]
      states[0]=states[1]=states[2]=null;
      byte[] retval=null;      
      
      try {
         if(generateTransient) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
            MarshalledValueOutputStream out = new MarshalledValueOutputStream(baos);
            marshallTransientState(rootNode, out);
            out.close();
            states[0] = baos.toByteArray();
            
            if (debug) {
               log.debug("returning the in-memory state (" + states[0].length + 
                         " bytes)");
            }
            
            // Return any state associated with the subtree but not stored in it
            if (cache instanceof PojoCache) {
               baos = new ByteArrayOutputStream(1024);
               out = new MarshalledValueOutputStream(baos);
               marshallAssociatedState(fqn, out);
               out.close();
               states[1] = baos.toByteArray();
               
               if (debug) {
                  log.debug("returning the associated state (" + 
                            states[1].length + " bytes)");
               }
            }
         }
      }
      catch(Throwable t) {
         log.error("failed getting the in-memory (transient) state", t);
         if (!suppressErrors) 
            throw t;
      }
      
      if (generatePersistent) {
         try {
            if (debug)
               log.debug("getting the persistent state");
            
            if (fqn.size() == 0)
               states[2] = cache.getCacheLoader().loadEntireState();
            else
               states[2] = ((ExtendedCacheLoader)cache.getCacheLoader()).loadState(fqn);
            
            if (debug) {
               log.debug("returning the persistent state (" + 
                         states[2].length + " bytes)");
            }
         }
         catch(Throwable t) {
            log.error("failed getting the persistent state", t);
            if (!suppressErrors)
               throw t;
         }
      }
   
      // Package everything into one byte[]
      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         MarshalledValueOutputStream out = new MarshalledValueOutputStream(baos);
         // Write out the version for reader can know how to integrate
         out.writeShort(STATE_TRANSFER_VERSION);
         out.writeObject(states);            
         out.close();
         retval = baos.toByteArray();
         
         log.info("returning the state for tree rooted in " + fqn.toString() +
                  "(" + retval.length + " bytes)");
         
         return retval;
      }
      catch(Throwable t) {
         log.error("failed serializing transient and persistent state", t);
         if (!suppressErrors)
            throw t;
         return retval;
      }
      
   }   


   /**
    * Do a preorder traversal: visit the node first, then the node's children
    * @param fqn Start node
    * @param out
    * @throws Exception
    */
   private void marshallTransientState(DataNode node, 
                                       ObjectOutputStream out) throws Exception 
   {      
      Map       attrs;
      NodeData  nd;

      // first handle the current node
      attrs=node.getData();
      if(attrs == null || attrs.size() == 0)
         nd=new NodeData(node.getFqn());
      else
         nd=new NodeData(node.getFqn(), attrs);
      out.writeObject(nd);

      // then visit the children
      Map children = node.getChildren();
      if(children == null)
         return;
      for(Iterator it=children.entrySet().iterator(); it.hasNext();) {
         Map.Entry entry = (Map.Entry) it.next();
         marshallTransientState((DataNode) entry.getValue(), out);
      }
   }
   
   /**
    * For each node in the internal reference map that is associated with the 
    * given Fqn, writes an Object[] to the stream containing the node's
    * name and the value of its sole attribute.  Does nothing if the Fqn is the 
    * root node (i.e. "/") or if it is in the internal reference area itself.
    */
   private void marshallAssociatedState(Fqn fqn, ObjectOutputStream out) 
         throws Exception
   {
      if (fqn == null 
            || fqn.size() == 0 
            || fqn.isChildOf(InternalDelegate.JBOSS_INTERNAL))
         return;

      DataNode refMapNode = cache.get(InternalDelegate.JBOSS_INTERNAL_MAP);
      
      Map children = null;
      if (refMapNode != null && (children = refMapNode.getChildren()) != null) {
         
         String targetFqn = ObjectUtil.getIndirectFqn(fqn.toString());
         
         Map.Entry entry;
         String key;
         DataNode value;
         for (Iterator iter = children.entrySet().iterator(); iter.hasNext();) {
            entry = (Map.Entry) iter.next();
            key = (String) entry.getKey();
            if (key.startsWith(targetFqn)) {
               value = (DataNode) entry.getValue();
               out.writeObject(new Object[] { key, value.get(key) });
            }
         }
      }
      
   }
}
