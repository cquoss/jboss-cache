/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.Version;
import org.jboss.cache.aop.InternalDelegate;
import org.jboss.cache.aop.PojoCache;
import org.jboss.cache.aop.util.ObjectUtil;
import org.jboss.cache.loader.ExtendedCacheLoader;
import org.jboss.cache.loader.NodeData;
import org.jboss.cache.util.ExposedByteArrayOutputStream;
import org.jboss.invocation.MarshalledValueOutputStream;

class StateTransferGenerator_140 implements StateTransferGenerator
{
   public static final short STATE_TRANSFER_VERSION = 
      Version.getVersionShort("1.4.0.GA");
   
   private Log log = LogFactory.getLog(getClass().getName());
   
   private TreeCache cache;
   private Set internalFqns;

   StateTransferGenerator_140(TreeCache cache)
   {
      this.cache        = cache;
      this.internalFqns = cache.getInternalFqns();
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
      int[] sizes = new int[3];
      byte[] retval = null;
      int lastSize;
      MarshalledValueOutputStream out;

      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(1024);
      try {
         initializeStateTransfer(baos);
         lastSize = baos.size();
      }
      catch (Throwable t)
      {
         log.error("failed initialing state transfer byte[]", t);
         if (!suppressErrors) 
            throw t;
         
         return null;
      }

      try {
         
         if(generateTransient) {
            out = new MarshalledValueOutputStream(baos);
            marshallTransientState(rootNode, out);
            out.close();
            sizes[0] = baos.size() - lastSize;
            lastSize = baos.size();
            if (debug) {
               log.debug("generated the in-memory state (" + sizes[0] + 
                         " bytes)");
            }
            // Return any state associated with the subtree but not stored in it
            if (cache instanceof PojoCache) {
               out = new MarshalledValueOutputStream(baos);
               marshallAssociatedState(fqn, out);
               out.close();
               sizes[1] = baos.size() - lastSize;
               lastSize = baos.size();
               if (debug) {
                  log.debug("returning the associated state (" + sizes[1] + 
                            " bytes)");
               }
            }
         }
      }
      catch(Throwable t) {
         log.error("failed getting the in-memory (transient) state", t);
         if (!suppressErrors) 
            throw t;
         
         // Reset the byte array and see if we can continue with persistent state
         // TODO reconsider this -- why are errors suppressed at all?
         sizes[0] = sizes[1] = 0;
         baos.reset();
         try {
            initializeStateTransfer(baos);
         }
         catch (Throwable t1) {
            log.error("failed re-initializing state transfer", t1);
            return null;
         }
      }
      
      if (generatePersistent) {
         try {
            if (debug)
               log.debug("getting the persistent state");
            byte[] persState = null;
            if (fqn.size() == 0)
               persState = cache.getCacheLoader().loadEntireState();
            else
               persState = ((ExtendedCacheLoader)cache.getCacheLoader()).loadState(fqn);
            
            if (persState != null) {
               sizes[2] = persState.length;
               baos.write(persState);
            }
            
            if (debug) {
               log.debug("generated the persistent state (" + sizes[2] + 
                         " bytes)");
            }
         }
         catch(Throwable t) {
            log.error("failed getting the persistent state", t);
            if (!suppressErrors)
               throw t;
            sizes[2] = 0;
         }
      }
   
      // Overwrite the placeholders used for the sizes of the state transfer
      // components with the correct values
      try {
         byte[] bytes = baos.getRawBuffer();
         overwriteInt(bytes, 8, sizes[0]);
         overwriteInt(bytes, 12, sizes[1]);
         overwriteInt(bytes, 16, sizes[2]);
         retval = bytes;
         
         log.info("returning the state for tree rooted in " + fqn.toString() +
                  "(" + retval.length + " bytes)");
         
         return retval;
      }
      catch(Throwable t) {
         log.error("failed serializing transient and persistent state", t);
         if (!suppressErrors)
            throw t;
         return null;
      }
      
   }
   
   private void initializeStateTransfer(OutputStream baos) throws IOException
   {
      MarshalledValueOutputStream out = new MarshalledValueOutputStream(baos);
      out.writeShort(STATE_TRANSFER_VERSION);
      // Write a placeholder for the 3 sizes we'll merge in later
      out.writeInt(0);
      out.writeInt(0);
      out.writeInt(0);
      out.close();
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
      if (internalFqns.contains(node.getFqn()))
         return;
      
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
   
   static void overwriteInt(byte[] bytes, int startpos, int newVal) 
   {   
       bytes[startpos]     = (byte) (newVal >>> 24);
       bytes[startpos + 1] = (byte) (newVal >>> 16);
       bytes[startpos + 2] = (byte) (newVal >>> 8);
       bytes[startpos + 3] = (byte) (newVal >>> 0);
   }
}
