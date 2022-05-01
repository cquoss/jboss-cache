/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.loader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.jboss.cache.Fqn;
import org.jboss.cache.buddyreplication.BuddyManager;
import org.jboss.cache.marshall.RegionManager;
import org.jboss.invocation.MarshalledValueInputStream;
import org.jboss.invocation.MarshalledValueOutputStream;

/**
 * A A <code>JDBCCacheLoader</code> that implements 
 * <code>ExtendedCacheLoader</code>.
 * 
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Id$
 */
public class JDBCExtendedCacheLoader extends JDBCCacheLoader implements ExtendedCacheLoader
{
   // --------------------------------------------------------  Instance Fields
   
   private RegionManager manager_;
   
   // -----------------------------------------------------------  Constructors

   /**
    * Create a new JDBCExtendedCacheLoader.
    * 
    */
   public JDBCExtendedCacheLoader()
   {
      super();
   }

   // ---------------------------------------------------  ExtendedCacheLoader

   public byte[] loadState(Fqn subtree) throws Exception
   {
      ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
      try
      {
         // Set the TCCL to any classloader registered for subtree
         setUnmarshallingClassLoader(subtree);
         
         ByteArrayOutputStream out_stream=new ByteArrayOutputStream(1024);
         ObjectOutputStream    out=new MarshalledValueOutputStream(out_stream);
         loadState(subtree, out);
         out.close();
         return out_stream.toByteArray();
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(currentCL);
      }
   }

   public void storeState(byte[] state, Fqn subtree) throws Exception
   {

      ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
      try
      {
         // Set the TCCL to any classloader registered for subtree
         setUnmarshallingClassLoader(subtree);
         
         ByteArrayInputStream in_stream=new ByteArrayInputStream(state);
         MarshalledValueInputStream in=new MarshalledValueInputStream(in_stream);
         NodeData nd;
   
         // remove entire existing state
         this.remove(subtree);
         
         boolean moveToBuddy = 
            subtree.isChildOf(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN) && subtree.size() > 1;
   
         // store new state
         Fqn fqn = null;
         while(in.available() > 0) 
         {
            nd=(NodeData)in.readObject();
            
            if (moveToBuddy)
               fqn = BuddyManager.getBackupFqn(subtree, nd.fqn);
            else
               fqn = nd.fqn;
   
            if(nd.attrs != null)
               this.put(fqn, nd.attrs, true); // creates a node with 0 or more attributes
            else
               this.put(fqn, null);  // creates a node with null attributes
         }
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(currentCL);
      }
   }

   public void setRegionManager(RegionManager manager)
   {
      this.manager_ = manager;

   }

   // ---------------------------------------------------  Overridden Methods

   /**
    * Overrides the {@link FileCacheLoader#loadEntireState() superclass method}
    * by taking advantage of any special classloader registered for the
    * root node.
    */
   public byte[] loadEntireState() throws Exception
   {
      return loadState(Fqn.fromString("/"));
   }

   /**
    * Overrides the {@link FileCacheLoader#storeEntireState() superclass method}
    * by taking advantage of any special classloader registered for the
    * root node.
    */
   public void storeEntireState(byte[] state) throws Exception
   {
      storeState(state, Fqn.fromString("/"));
   }
   



   // -------------------------------------------------------  Private Methods

   /**
    * Checks the RegionManager for a classloader registered for the 
    * given, and if found sets it as the TCCL
    * 
    * @param subtree
    */
   private void setUnmarshallingClassLoader(Fqn subtree)
   {
      if (manager_ != null)
      {
         manager_.setUnmarshallingClassLoader(subtree);
      }
   }

}
