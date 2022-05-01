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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Abstract Marshaller for JBoss Cache.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public abstract class Marshaller
{
   protected boolean useRegionBasedMarshalling;
   protected RegionManager regionManager;
   protected boolean defaultInactive;
   private static Log log = LogFactory.getLog(Marshaller.class);

   /**
    * Map<GlobalTransaction, Fqn> for prepared tx that have not committed
    */
   private ConcurrentHashMap transactions = new ConcurrentHashMap(16);

   protected void init(RegionManager manager, boolean defaultInactive, boolean useRegionBasedMarshalling)
   {
      this.useRegionBasedMarshalling = useRegionBasedMarshalling;
      this.defaultInactive = defaultInactive;
      this.regionManager = manager;
   }

   /**
    * Implementation classes will need to marshall the object passed in and write the object
    * into the given stream.
    *
    * @param obj
    * @param out
    * @throws Exception
    */
   public abstract void objectToStream(Object obj, ObjectOutputStream out) throws Exception;

   /**
    * Implementation classes will need to parse the given stream and create an object from it.
    *
    * @param in
    * @throws Exception
    */
   public abstract Object objectFromStream(ObjectInputStream in) throws Exception;


   /**
    * This is "replicate" call with a single MethodCall argument.
    *
    * @param call
    */
   protected String extractFqnFromMethodCall(JBCMethodCall call)
   {
      JBCMethodCall c0 = (JBCMethodCall) call.getArgs()[0];
      return extractFqn(c0);
   }

   /**
    * This is "replicate" call with a list of MethodCall argument.
    *
    * @param call
    */
   protected String extractFqnFromListOfMethodCall(JBCMethodCall call)
   {
      Object[] args = call.getArgs();
      // We simply pick the first one and assume everyone will need to operate under the same region!
      JBCMethodCall c0 = (JBCMethodCall) ((List) args[0]).get(0);
      return extractFqn(c0);
   }

   protected String extractFqn(JBCMethodCall methodCall)
   {
      if (methodCall == null)
      {
         throw new NullPointerException("method call is null");
      }

      Method meth = methodCall.getMethod();
      String fqnStr = null;
      Object[] args = methodCall.getArgs();
      switch (methodCall.getMethodId())
      {
         case MethodDeclarations.optimisticPrepareMethod_id:
         case MethodDeclarations.prepareMethod_id:
            // Prepare method has a list of modifications. We will just take the first one and extract.
            List modifications = (List) args[1];
            fqnStr = extractFqn((JBCMethodCall) modifications.get(0));

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
         case MethodDeclarations.evictNodeMethodLocal_id:
         case MethodDeclarations.evictVersionedNodeMethodLocal_id:
            fqnStr = args[0].toString();
            break;
         case MethodDeclarations.dataGravitationCleanupMethod_id:
            fqnStr = args[1].toString();
            break;
         case MethodDeclarations.remoteAnnounceBuddyPoolNameMethod_id:
         case MethodDeclarations.remoteAssignToBuddyGroupMethod_id:
         case MethodDeclarations.remoteRemoveFromBuddyGroupMethod_id:
            break;
         case MethodDeclarations.replicateMethod_id:
         case MethodDeclarations.clusteredGetMethod_id:
            // possible when we have a replication queue or a clustered get call
            fqnStr = extractFqn((JBCMethodCall) args[0]);
            break;
         default :
            if (MethodDeclarations.isCrudMethod(meth))
            {
               fqnStr = args[1].toString();
            }
            else if (MethodDeclarations.isGetMethod(methodCall.getMethodId()))
            {
               fqnStr = args[0].toString();
            }
            else
            {
               throw new IllegalArgumentException("Marshaller.extractFqn(): Unknown method call id: " + methodCall.getMethodId());
            }
            break;
      }

      if (log.isTraceEnabled())
      {
         log.trace("extract(): received " + methodCall + "extracted fqn: " + fqnStr);
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
      return regionManager.getRegion(fqn);
   }

   /**
    * Register the specific classloader under the <code>fqn</code> region.
    *
    * @param fqn
    * @param cl
    * @throws RegionNameConflictException thrown if there is a conflict in region definition.
    */
   public void registerClassLoader(String fqn, ClassLoader cl)
           throws RegionNameConflictException
   {
      if (!useRegionBasedMarshalling) return;
      Region existing = regionManager.getRegion(fqn);
      if (existing == null)
      {
         regionManager.createRegion(fqn, cl, defaultInactive);
      }
      else
      {
         existing.setClassLoader(cl);
      }
   }

   /**
    * Un-register the class loader. Caller will need to call this when the application is out of scope.
    * Otherwise, the class loader will not get gc.
    *
    * @param fqn
    */
   public void unregisterClassLoader(String fqn) throws RegionNotFoundException
   {
      if (!useRegionBasedMarshalling) return;
      // Brian -- we no longer remove the region, as regions
      // also have the inactive property
      // TODO how to clear regions from the RegionManager??
      //regionManager.removeRegionToProcess(fqn);
      Region region = regionManager.getRegion(fqn);
      if (region != null)
      {
         region.setClassLoader(null);
      }
   }

   /**
    * Gets the classloader previously registered for <code>fqn</code> by
    * a call to {@link #registerClassLoader(String, ClassLoader)}.
    *
    * @param fqn the fqn
    * @return the classloader associated with the cache region rooted by
    *         <code>fqn</code>, or <code>null</code> if no classloader has
    *         been associated with the region.
    * @throws RegionNotFoundException
    */
   public ClassLoader getClassLoader(String fqn) throws RegionNotFoundException
   {
      if (!useRegionBasedMarshalling) return null;
      ClassLoader result = null;
      Region region = regionManager.getRegion(fqn);
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
      if (!useRegionBasedMarshalling) return;
      if (regionManager.hasRegion(fqn)) // tests for an exact match
      {
         Region region = regionManager.getRegion(fqn);
         if (!defaultInactive && region.getClassLoader() == null)
         {
            // This region's state will no match that of a non-existent one
            // So, there is no reason to keep this region any more
            regionManager.removeRegion(fqn);
         }
         else
         {
            region.activate();
         }
      }
      else if (defaultInactive)
      {
         // "Active" region is not the default, so create a region
         // May throw RegionNameConflictException
         regionManager.createRegion(fqn, null, false);
      }
      else
      {
         // "Active" is the default, so no need to create a region,
         // but must check if this one conflicts with others
         regionManager.checkConflict(fqn);
      }
   }

   /**
    * Disables unmarshalling of replication messages for the region
    * rooted in the given Fqn.
    *
    * @param fqn
    * @throws RegionNameConflictException if there is a conflict in region definition.
    */
   public void inactivate(String fqn) throws RegionNameConflictException
   {
      if (!useRegionBasedMarshalling) return;
      if (regionManager.hasRegion(fqn)) // tests for an exact match
      {
         Region region = regionManager.getRegion(fqn);
         if (defaultInactive && region.getClassLoader() == null)
         {
            // This region's state will no match that of a non-existent one
            // So, there is no reason to keep this region any more
            regionManager.removeRegion(fqn);
         }
         else
         {
            region.inactivate();
         }
      }
      else if (!defaultInactive)
      {
         regionManager.createRegion(fqn, null, true);
      }
      else
      {
         // nodes are by default inactive, so we don't have to create one
         // but, we must check in case fqn is in conflict with another region
         regionManager.checkConflict(fqn);
      }

   }

   /**
    * Gets whether unmarshalling has been disabled for the region
    * rooted in the given Fqn.
    *
    * @param fqn
    * @return <code>true</code> if unmarshalling is disabled;
    *         <code>false</code> otherwise.
    * @see #activate
    * @see #inactivate
    */
   public boolean isInactive(String fqn)
   {
      if (!useRegionBasedMarshalling) return false;
      boolean result = defaultInactive;

      Region region = regionManager.getRegion(fqn);
      if (region != null)
      {
         result = region.isInactive();
      }

      return result;
   }
}
