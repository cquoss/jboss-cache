/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeNode;
import org.jboss.cache.TreeCache;
import org.jboss.cache.aop.InternalDelegate;
import org.jboss.cache.aop.PojoCache;
import org.jboss.cache.eviction.EvictedEventNode;
import org.jboss.cache.eviction.Region;
import org.jboss.cache.eviction.RegionManager;
import org.jboss.cache.factories.NodeFactory;
import org.jboss.cache.loader.CacheLoader;
import org.jboss.cache.loader.ExtendedCacheLoader;
import org.jboss.cache.loader.NodeData;
import org.jboss.invocation.MarshalledValueInputStream;

class StateTransferIntegrator_1241 implements StateTransferIntegrator
{
   /** Number of bytes at the beginning of the state transfer byte[]
    *  utilized by meta-information about the composition of the byte[] 
    *  (6 for stream header, 2 for version short, 
    *  3 * 4 for lengths of the state components, 2 bytes for close)   
    */
   private static final int HEADER_LENGTH = 6 + 2 + 4 + 4 + 4;// + 2;
   
   private Log log = LogFactory.getLog(getClass().getName());
   
   private TreeCache cache;
   private Fqn     targetFqn;
   private byte[]  state;
   private int     transientSize;
   private int     associatedSize;
   private int     persistentSize;
   private boolean transientSet;
   private NodeFactory factory;
   private byte nodeType;
   
   
   StateTransferIntegrator_1241(byte[] state, Fqn targetFqn,  
                               TreeCache cache) throws Exception
   {
      this.targetFqn = targetFqn;
      this.cache     = cache;
      this.state      = state;
      this.factory = NodeFactory.getInstance();
      this.nodeType = cache.isNodeLockingOptimistic() 
                                    ? NodeFactory.NODE_TYPE_OPTIMISTIC_NODE 
                                    : NodeFactory.NODE_TYPE_TREENODE;
      
      ByteArrayInputStream bais = new ByteArrayInputStream(state);
      MarshalledValueInputStream in = new MarshalledValueInputStream(bais);
      in.readShort(); // the version, which we discard
      transientSize  = in.readInt();
      associatedSize = in.readInt();
      persistentSize = in.readInt();
      in.close();
      if (log.isTraceEnabled()) {
            log.trace("transient state: " + transientSize + " bytes");
            log.trace("associated state: " + associatedSize + " bytes");
            log.trace("persistent state: " + persistentSize + " bytes");
      }
   }
   
   public void integrateTransientState(DataNode target, ClassLoader cl) 
      throws Exception
   {
      if (transientSize > 0) {
         
         ClassLoader oldCL = null;         
         try {
            if (cl != null) {
               oldCL = Thread.currentThread().getContextClassLoader(); 
               Thread.currentThread().setContextClassLoader(cl);
            }
            
            if (log.isTraceEnabled())
               log.trace("integrating transient state for " + target);
            
            integrateTransientState(target);
            
            transientSet = true;
            
            if (log.isTraceEnabled())
               log.trace("transient state successfully integrated for " + 
                         targetFqn);
            
            // 3. Set the associated state.  We only do this if the normal
            // transient state was set.
            integrateAssociatedState();
         }
         finally {
            if (!transientSet) {
               // Clear any existing state from the targetRoot
               target.clear();
               target.removeAllChildren();
            }
            
            if (oldCL != null)
               Thread.currentThread().setContextClassLoader(oldCL);            
         }
      }
   }
   
   private void integrateAssociatedState() throws Exception
   {
      if (associatedSize > 0 && cache instanceof PojoCache) {
         
         DataNode refMapNode = cache.get(InternalDelegate.JBOSS_INTERNAL_MAP);

         ByteArrayInputStream in_stream=new ByteArrayInputStream(state, HEADER_LENGTH + transientSize, associatedSize);
         MarshalledValueInputStream in=new MarshalledValueInputStream(in_stream);
         
         try {
            Object[] nameValue;
            while ((nameValue = (Object[]) in.readObject()) != null) {
               TreeNode target = refMapNode.getChild(nameValue[0]);
               
               if (target == null) {
                  // Create the node
                  Fqn fqn = new Fqn(InternalDelegate.JBOSS_INTERNAL_MAP, nameValue[0]);
                  target = factory.createDataNode(nodeType, 
                                                  nameValue[0], 
                                                  fqn, 
                                                  refMapNode, 
                                                  null, 
                                                  true,
                                                  cache);
                  refMapNode.addChild(nameValue[0], target);
               }
               
               target.put(nameValue[0], nameValue[1]);
            }
         }
         catch (EOFException eof) {
            // all done
         }
         
         if (log.isTraceEnabled())
            log.trace("associated state successfully integrated for " + targetFqn);
      }
      else if (log.isTraceEnabled()) {
         log.trace("No need to integrate associated state for " + targetFqn);
      }
   }
   
   public void integratePersistentState() throws Exception
   {
      if(persistentSize > 0) {
         CacheLoader loader = cache.getCacheLoader();
         if(loader == null) {
            log.error("cache loader is null, cannot set persistent state");
         }
         else if (targetFqn.size() == 0){
            if (log.isTraceEnabled())
               log.trace("setting the persistent state");
            byte[] persistentState = getPersistentState();
            loader.storeEntireState(persistentState);
            if (log.isTraceEnabled())
               log.trace("setting the persistent state was successful");
         }
         else if (loader instanceof ExtendedCacheLoader) {
            if (log.isTraceEnabled())
               log.trace("setting the persistent state");
            // cache_loader.remove(Fqn.fromString("/"));
            byte[] persistentState = getPersistentState();
            ((ExtendedCacheLoader) loader).storeState(persistentState, 
                                                      targetFqn);
            if (log.isTraceEnabled())
               log.trace("setting the persistent state was successful");
         }            
         else {
            log.error("cache loader does not implement ExtendedCacheLoader, " +
                      "cannot set persistent state");
         }
      }
   }
   
   private void integrateTransientState(DataNode target)
         throws IOException, ClassNotFoundException
   {
      target.removeAllChildren();
      
      ByteArrayInputStream in_stream=new ByteArrayInputStream(state, HEADER_LENGTH, transientSize);
      MarshalledValueInputStream in=new MarshalledValueInputStream(in_stream);
      
      // Read the first NodeData and integrate into our target
      NodeData nd = (NodeData) in.readObject();
      Map attrs = nd.getAttributes();
      if (attrs != null)
         target.put(attrs, true);
      else
         target.clear();
      
      RegionManager erm = cache.getEvictionRegionManager();
      if (erm != null)
      {
         Region[] regions = erm.getRegions();
         if (regions == null || regions.length == 0)
            erm = null;
      }
      integrateStateTransferChildren(target, in, erm);
      
      in.close();      
   }
   
   private NodeData integrateStateTransferChildren(DataNode parent, 
                                                   ObjectInputStream in,
                                                   RegionManager erm)
         throws IOException, ClassNotFoundException
   {
      int parent_level = parent.getFqn().size();
      int target_level = parent_level + 1;
      Fqn fqn;
      int size;
      Object name;
      try
      {
         NodeData nd = (NodeData) in.readObject();
         while (nd != null) {            
            fqn = nd.getFqn();
            size = fqn.size();
            if (size <= parent_level)
               return nd;
            else if (size > target_level)
               throw new IllegalStateException("NodeData " + fqn +
                                               " is not a direct child of " +
                                               parent.getFqn());

            name = fqn.get(size - 1);
            
            Map attrs = nd.getAttributes();
            
            // We handle this NodeData.  Create a DataNode and
            // integrate its data            
            DataNode target = factory.createDataNode(nodeType, 
                                                     name, 
                                                     fqn, 
                                                     parent, 
                                                     attrs, 
                                                     true,
                                                     cache);
            parent.addChild(name, target);
            
            // Make sure any eviction policy is aware of this node
            if (erm != null)
            {
               Region region = null;
               try
               {
                  region = erm.getRegion(fqn);
               }
               catch (RuntimeException e)
               {
                  if (erm.hasRegion(RegionManager.DEFAULT_REGION))
                     throw e;
                  // else the fqn is not associated with an eviction region
               }
               
               if (region != null)
               {
                  region.putNodeEvent(new EvictedEventNode(fqn, EvictedEventNode.ADD_NODE_EVENT, 
                                                           attrs == null ? 0 : attrs.size()));
               }
            }
            
            // Recursively call, which will walk down the tree
            // and return the next NodeData that's a child of our parent
            nd = integrateStateTransferChildren(target, in, erm);
         }
      }
      catch (EOFException eof) {
         // all done
      }
      
      return null;
   }
   
   private byte[] getPersistentState()
   {
      byte[] result = new byte[persistentSize];
      System.arraycopy(state, HEADER_LENGTH + transientSize + associatedSize, result, 0, persistentSize);
      return result;
   }
}
