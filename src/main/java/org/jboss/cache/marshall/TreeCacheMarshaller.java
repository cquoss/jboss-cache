/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.marshall;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.buddyreplication.BuddyManager;
import org.jboss.invocation.MarshalledValueInputStream;
import org.jboss.invocation.MarshalledValueOutputStream;
import org.jgroups.blocks.RpcDispatcher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.List;

/**
 * <p>
 * Marshaller implementation that does aplication specific marshalling in the <em>JGroups</em> <code>RpcDispatcher</code>
 * level. Application
 * that runs on specific class loader will only need to register beforehand with TreeCache the class loader
 * needed under the specific <code>fqn</code> region. Note again that this marshalling policy is region based.
 * Anything that falls under that region will use the registered class loader. We also handle the region conflict
 * during registeration time as well. For <code>fqn</code> that does
 * not belong to any region, the default (system) class loader will be used.</p>
 *
 * @author Ben Wang
 *         Date: Aug 9, 2005
 * @version $Id: TreeCacheMarshaller.java 2043 2006-06-06 10:20:05Z msurtani $
 */
public class TreeCacheMarshaller implements RpcDispatcher.Marshaller {

   protected RegionManager manager_;
   protected boolean defaultInactive_;

   /** Map<GlobalTransaction, Fqn> for prepared tx that have not committed */
   private ConcurrentHashMap transactions=new ConcurrentHashMap(16);

   private Log log_=LogFactory.getLog(TreeCacheMarshaller.class);

   public TreeCacheMarshaller()
   {
   }

   public TreeCacheMarshaller(RegionManager manager,
                              boolean defaultInactive)
   {
       if (manager == null)
       {
           throw new IllegalArgumentException("manager cannot be null");
       }

      this.manager_ = manager;
      this.defaultInactive_ = defaultInactive;
   }


    public RegionManager getManager()
    {
        return manager_;
    }

    public void setManager(RegionManager manager)
    {
        this.manager_ = manager;
    }

    public boolean isDefaultInactive()
    {
        return defaultInactive_;
    }

    public void setDefaultInactive(boolean defaultInactive)
    {
        this.defaultInactive_ = defaultInactive;
    }

    /**
    * Register the specific classloader under the <code>fqn</code> region.
    * @param fqn
    * @param cl
    * @throws RegionNameConflictException thrown if there is a conflict in region definition.
    */
   public void registerClassLoader(String fqn, ClassLoader cl)
           throws RegionNameConflictException
   {
      Region existing = manager_.getRegion(fqn);
      if (existing == null) {
         manager_.createRegion(fqn, cl, defaultInactive_);
      }
      else {
         existing.setClassLoader(cl);
      }
   }

   /**
    * Un-register the class loader. Caller will need to call this when the application is out of scope.
    * Otherwise, the class loader will not get gc.
    * @param fqn
    */
   public void unregisterClassLoader(String fqn) throws RegionNotFoundException
   {
      // Brian -- we no longer remove the region, as regions
      // also have the inactive property
      // TODO how to clear regions from the RegionManager??
      //manager_.removeRegionToProcess(fqn);
      Region region = manager_.getRegion(fqn);
       if (region != null)
       {
           region.setClassLoader(null);
       }
   }

   /**
    * Gets the classloader previously registered for <code>fqn</code> by
    * a call to {@link #registerClassLoader(String, ClassLoader)}.
    *
    * @param fqn  the fqn
    * @return  the classloader associated with the cache region rooted by
    *          <code>fqn</code>, or <code>null</code> if no classloader has
    *          been associated with the region.
    *
    * @throws RegionNotFoundException
    */
   public ClassLoader getClassLoader(String fqn) throws RegionNotFoundException
   {
      ClassLoader result = null;
      Region region = manager_.getRegion(fqn);
      if (region != null)
      {
         result = region.getClassLoader();
      }
      return result;
   }

   /**
    * Activates unmarshalling of replication messages for the region
    * rooted in the given Fqn.
    *
    * @param fqn
    */
   public void activate(String fqn) throws RegionNameConflictException
   {

      if (manager_.hasRegion(fqn)) // tests for an exact match
      {
         Region region = manager_.getRegion(fqn);
         if (defaultInactive_ == false && region.getClassLoader() == null)
         {
            // This region's state will no match that of a non-existent one
            // So, there is no reason to keep this region any more
            manager_.removeRegion(fqn);
         }
         else
         {
            region.activate();
         }
      }
      else if (defaultInactive_)
      {
         // "Active" region is not the default, so create a region
         // May throw RegionNameConflictException
         manager_.createRegion(fqn, null, false);
      }
      else
      {
         // "Active" is the default, so no need to create a region,
         // but must check if this one conflicts with others
         manager_.checkConflict(fqn);
      }
   }

   /**
    * Disables unmarshalling of replication messages for the region
    * rooted in the given Fqn.
    *
    * @param fqn
    *
    * @throws RegionNameConflictException if there is a conflict in region definition.
    */
   public void inactivate(String fqn) throws RegionNameConflictException
   {
      if (manager_.hasRegion(fqn)) // tests for an exact match
      {
         Region region = manager_.getRegion(fqn);
         if (defaultInactive_ && region.getClassLoader() == null)
         {
            // This region's state will no match that of a non-existent one
            // So, there is no reason to keep this region any more
            manager_.removeRegion(fqn);
         }
         else
         {
            region.inactivate();
         }
      }
      else if (defaultInactive_ == false)
      {
         manager_.createRegion(fqn, null, true);
      }
      else
      {
         // nodes are by default inactive, so we don't have to create one
         // but, we must check in case fqn is in conflict with another region
         manager_.checkConflict(fqn);
      }

   }

   /**
    * Gets whether unmarshalling has been disabled for the region
    * rooted in the given Fqn.
    *
    * @param fqn
    *
    * @return  <code>true</code> if unmarshalling is disabled;
    *          <code>false</code> otherwise.
    *
    * @see #activate
    * @see #inactivate
    */
   public boolean isInactive(String fqn)
   {
      boolean result = defaultInactive_;

      Region region = manager_.getRegion(fqn);
       if (region != null)
       {
           result = region.isInactive();
       }

      return result;
   }

   /* ----------------- Begining of  Callbacks for RpcDispatcher.Marshaller ---------------------- */

   /**
    * Idea is to write specific fqn information in the header such that during unm-marshalling we know
    * which class loader to use.
    * @param o
    * @return
    * @throws Exception
    */
   public byte[] objectToByteBuffer(Object o) throws Exception {
      /**
       * Object is always MethodCall, it can be either: replicate or replicateAll (used in async repl queue)
       * 1. replicate. Argument is another MethodCall. The followings are the one that we need to handle:
       * 2. replicateAll. List of MethodCalls. We can simply repeat the previous step by extract the first fqn only.
       */
      JBCMethodCall call = (JBCMethodCall) o; // either "replicate" or "replicateAll" now.
      String fqn;
      switch (call.getMethodId())
      {
         case MethodDeclarations.replicateMethod_id:
            fqn = extractFqnFromMethodCall(call);
            break;
         case MethodDeclarations.replicateAllMethod_id:
            fqn = extractFqnFromListOfMethodCall(call);
            break;
         default :
            throw new IllegalStateException("TreeCacheMarshaller.objectToByteBuffer(): MethodCall name is either not "
                  + " replicate or replicateAll but : " +call.getName());
      }

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new MarshalledValueOutputStream(bos);

      // Extract fqn and write it out in fixed format
      if(fqn == null) fqn = "NULL"; // can't write null. tis can be commit.
      oos.writeUTF(fqn);
      // Serialize the rest of MethodCall object
      oos.writeObject(o);
      if (log_.isTraceEnabled()) {
         log_.trace("send");
         log_.trace(getColumnDump(bos.toByteArray()));
      }
      return bos.toByteArray();
   }

   /**
    * This is the un-marshalling step. We will read in the fqn and thus obtain the user-specified classloader
    * first.
    * @param bytes
    * @return
    * @throws Exception
    */
   public Object objectFromByteBuffer(byte[] bytes) throws Exception {
      if (log_.isTraceEnabled()) {
         log_.trace("recv");
         log_.trace(getColumnDump(bytes));
      }
      ByteArrayInputStream is = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = new MarshalledValueInputStream(is);

      // Read the fqn first
      String fqn = ois.readUTF();
      ClassLoader oldTcl = null;;
      Region region = null;
      if(fqn != null && !fqn.equals("NULL"))
      {
         // obtain a region from RegionManager, if not, will use default.
         region = getRegion(fqn);

         if(region != null)
         {
            // If the region has been marked inactive, we still have
            // to return a MethodCall or RpcDispatcher will log an Error.
            // So, return a call to the TreeCache "_notifyCallOnInactive" method
            if (region.getStatus() == Region.STATUS_INACTIVE)
            {
                if (log_.isTraceEnabled())
                {
                    log_.trace("objectFromByteBuffer(): fqn: " + fqn + " is in the inactive default region");
                }

               return MethodCallFactory.create(MethodDeclarations.notifyCallOnInactiveMethod,
                                     new Object[] { fqn} );
            }

            // If the region has an associated CL, read the value using it
            ClassLoader cl = region.getClassLoader();
            if (cl != null)
            {
               oldTcl = Thread.currentThread().getContextClassLoader();
               Thread.currentThread().setContextClassLoader(cl);

                if (log_.isTraceEnabled())
                {
                    log_.trace("objectFromByteBuffer(): fqn: " + fqn + " Will use customed class loader " + cl);
                }
            }
         }
         else if (defaultInactive_)
         {
            // No region but default inactive means region is inactive

             if (log_.isTraceEnabled())
             {
                 log_.trace("objectFromByteBuffer(): fqn: " + fqn + " is in an inactive region");
             }

            return MethodCallFactory.create(MethodDeclarations.notifyCallOnInactiveMethod,
                  new Object[] { fqn} );
         }
      }

      // Read the MethodCall object using specified class loader
      Object obj = null;
      try
      {
         obj = ois.readObject();
      } finally
      {
          if (oldTcl != null)
          {
              Thread.currentThread().setContextClassLoader(oldTcl);
          }
      }

       if (obj == null)
       {
           throw new MarshallingException("Read null object with fqn: " + fqn);
       }

      // If the region is queuing messages, wrap the method call
      // and pass it to the enqueue method
      if (region != null && region.isQueueing())
      {
         obj = MethodCallFactory.create(MethodDeclarations.enqueueMethodCallMethod,
                              new Object[] { region.getFqn(), obj });
      }

      return obj;
   }

   /**
    * This is "replicate" call with a single MethodCall argument.
    * @param call
    */
   protected String extractFqnFromMethodCall(JBCMethodCall call)
   {
      JBCMethodCall c0 = (JBCMethodCall)call.getArgs()[0];
      return extractFqn(c0);
   }

   /**
    * This is "replicate" call with a list of MethodCall argument.
    * @param call
    */
   protected String extractFqnFromListOfMethodCall(JBCMethodCall call)
   {
      Object[] args = call.getArgs();
      // We simply pick the first one and assume everyone will need to operate under the same region!
      JBCMethodCall c0 = (JBCMethodCall)((List)args[0]).get(0);
      return extractFqn(c0);
   }

   protected String extractFqn(JBCMethodCall method_call)
   {
       if (method_call == null)
       {
           throw new NullPointerException("method call is null");
       }

      Method meth=method_call.getMethod();
      String fqnStr = null;
      Object[] args = method_call.getArgs();
      switch (method_call.getMethodId())
      {
         case MethodDeclarations.optimisticPrepareMethod_id: 
         case MethodDeclarations.prepareMethod_id:
            // Prepare method has a list of modifications. We will just take the first one and extract.
            List modifications=(List)args[1];
            fqnStr = extractFqn((JBCMethodCall)modifications.get(0));
             
            // the last arg of a prepare call is the one-phase flag
            boolean one_phase_commit = ((Boolean) args[args.length - 1]).booleanValue();

            // If this is two phase commit, map the FQN to the GTX so
            // we can find it when the commit/rollback comes through
             if (!one_phase_commit)
             {
                 transactions.put(args[0], fqnStr);
             }
            break;
         case MethodDeclarations.rollbackMethod_id:
         case MethodDeclarations.commitMethod_id:
            // We stored the fqn in the transactions map during the prepare phase
            fqnStr = (String) transactions.remove(args[0]);
            break;
         case MethodDeclarations.getPartialStateMethod_id:
         case MethodDeclarations.dataGravitationMethod_id:
            Fqn fqn = (Fqn) args[0];
            fqnStr = fqn.toString();
            break;
         case MethodDeclarations.dataGravitationCleanupMethod_id:
            Fqn fqn1 = (Fqn) args[1];
            fqnStr = fqn1.toString();
            break;
         case MethodDeclarations.remoteAnnounceBuddyPoolNameMethod_id:
         case MethodDeclarations.remoteAssignToBuddyGroupMethod_id:
         case MethodDeclarations.remoteRemoveFromBuddyGroupMethod_id:
            break;
         default :
            if (MethodDeclarations.isCrudMethod(meth)) 
            {
               Fqn fqn2 = (Fqn)args[1];
               fqnStr = fqn2.toString();
            }
            else 
            {
               throw new IllegalArgumentException("TreeCacheMarshaller.extractFqn(): Unknown method call name: "
                     +meth.getName());
            }
            break;
         
      }
      
       if (log_.isTraceEnabled())
       {
           log_.trace("extract(): received " + method_call + "extracted fqn: " + fqnStr);
       }

      return fqnStr;
   }
   
   protected Region getRegion(String fqnString)
   {
      Fqn fqn = Fqn.fromString(fqnString);
      
      if (BuddyManager.isBackupFqn(fqn))
      {
         // Strip out the buddy group portion
         fqn = fqn.getFqnChild(2, fqn.size());
      }
      return manager_.getRegion(fqn);
   }

   String getColumnDump(byte buffer[])
   {
       int col = 16; 
       int length = buffer.length;
       int offs = 0;
       StringBuffer sb = new StringBuffer(length * 4);
       StringBuffer tx = new StringBuffer();
       for (int i=0; i<length; i++) {
           if (i % col == 0) {
               sb.append(tx).append('\n');
               tx.setLength(0);
           }
           byte b = buffer[i + offs];
           if (Character.isISOControl((char) b))
           {
               tx.append('.');
           }
           else
           {
               tx.append((char) b);
           }
           appendHex(sb, b);
           sb.append(' ');
       }
       int remain = col - (length % col);
       if (remain != col) {
           for (int i = 0; i < remain * 3; i++)
           {
               sb.append(' ');
           }
       }
       sb.append(tx);
       return sb.toString();
   }

    private static void appendHex(StringBuffer sb, byte b) {
        sb.append(Character.forDigit((b >> 4) & 0x0f, 16));
        sb.append(Character.forDigit(b & 0x0f, 16));
    }

}
