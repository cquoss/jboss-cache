/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.marshall;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jboss.cache.TreeCache;
import org.jboss.cache.buddyreplication.BuddyGroup;
import org.jboss.cache.optimistic.DataVersion;
import org.jboss.cache.rpc.RpcTreeCache;
import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;
import org.jgroups.stack.IpAddress;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class containing Method and Method id definitions as well methods
 * allowing lookup operations both ways.
 *
 * @author <a href="galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @version $Revision: 3624 $
 */
public class MethodDeclarations
{
    private static Log log = LogFactory.getLog(MethodDeclarations.class);

    private static Set crud_methods = new HashSet();
    private static Set crud_method_ids = new HashSet();
   private static Set optimisticPutMethods = new HashSet();

    // maintain a list of method IDs that correspond to Methods in TreeCache
    private static Map methods = new HashMap();

    // and for reverse lookup
    private static Map methodIds = new HashMap();

    public static final Method putDataMethodLocal;

    public static final Method putDataEraseMethodLocal;

    public static final Method putKeyValMethodLocal;

    public static final Method putFailFastKeyValueMethodLocal;

    public static final Method removeNodeMethodLocal;

    public static final Method removeKeyMethodLocal;

    public static final Method removeDataMethodLocal;

    public static final Method evictNodeMethodLocal;

    public static final Method evictVersionedNodeMethodLocal;

    // public static Method evictKeyValueMethodLocal=null;
    public static final Method prepareMethod;

    public static final Method commitMethod;

    public static final Method rollbackMethod;

    public static final Method replicateMethod;

    public static final Method replicateAllMethod;

    // public static Method addChildMethod=null;
    public static final Method addChildMethodLocal;

    public static final Method getKeyValueMethodLocal;

    public static final Method getNodeMethodLocal;

    public static final Method getKeysMethodLocal;

    public static final Method getChildrenNamesMethodLocal;

    public static final Method getDataMapMethodLocal;

    public static final Method existsMethod;

    public static final Method releaseAllLocksMethodLocal;

    public static final Method printMethodLocal;

    public static final Method lockMethodLocal;

    public static final Method optimisticPrepareMethod;

    public static final Method getPartialStateMethod;

    public static final Method enqueueMethodCallMethod;

    public static final Method notifyCallOnInactiveMethod;

    public static final Method clusteredGetMethod;

    public static final Method remoteAssignToBuddyGroupMethod;

    public static final Method remoteRemoveFromBuddyGroupMethod;

    public static final Method remoteAnnounceBuddyPoolNameMethod;

    public static final Method dataGravitationCleanupMethod;

    public static final Method dataGravitationMethod;

   // these are basic crud methods that are version-aware - JBCACHE-843.

   public static final Method putDataVersionedMethodLocal;

   public static final Method putDataEraseVersionedMethodLocal;

   public static final Method putKeyValVersionedMethodLocal;

   public static final Method removeNodeVersionedMethodLocal;

   public static final Method removeKeyVersionedMethodLocal;

   public static final Method removeDataVersionedMethodLocal;



    //not all of these are used for RPC - trim accordingly.
    public static final int putDataMethodLocal_id = 1;

    public static final int putDataEraseMethodLocal_id = 2;

    public static final int putKeyValMethodLocal_id = 3;

    public static final int putFailFastKeyValueMethodLocal_id = 4;

    public static final int removeNodeMethodLocal_id = 5;

    public static final int removeKeyMethodLocal_id = 6;

    public static final int removeDataMethodLocal_id = 7;

    public static final int evictNodeMethodLocal_id = 8;

    public static final int evictVersionedNodeMethodLocal_id = 9;

    public static final int prepareMethod_id = 10;

    public static final int commitMethod_id = 11;

    public static final int rollbackMethod_id = 12;

    public static final int replicateMethod_id = 13;

    public static final int replicateAllMethod_id = 14;

    public static final int addChildMethodLocal_id = 15;

    public static final int existsMethod_id = 16;

    public static final int releaseAllLocksMethodLocal_id = 17;

    public static final int optimisticPrepareMethod_id = 18;

    public static final int getPartialStateMethod_id = 19;

    public static final int enqueueMethodCallMethod_id = 20;

    public static final int notifyCallOnInactiveMethod_id = 21;

    public static final int clusteredGetMethod_id = 22;

    public static final int getChildrenNamesMethodLocal_id = 23;

    public static final int getDataMapMethodLocal_id = 24;

    public static final int getKeysMethodLocal_id = 25;

    public static final int getKeyValueMethodLocal_id = 26;

    public static final int dispatchRpcCallMethod_id = 27;

    public static final int remoteAnnounceBuddyPoolNameMethod_id = 28;

    public static final int remoteAssignToBuddyGroupMethod_id = 29;

    public static final int remoteRemoveFromBuddyGroupMethod_id = 30;

    /* Method id added as they did not exist before refactoring */
    public static final int getNodeMethodLocal_id = 31;

    public static final int printMethodLocal_id = 32;

    public static final int lockMethodLocal_id = 33;

    public static final int dataGravitationCleanupMethod_id = 34;

    public static final int dataGravitationMethod_id = 35;

   // these are basic crud methods that are version-aware - JBCACHE-843.

   public static final int putDataVersionedMethodLocal_id = 36;

   public static final int putDataEraseVersionedMethodLocal_id = 37;

   public static final int putKeyValVersionedMethodLocal_id = 38;

   public static final int removeNodeVersionedMethodLocal_id = 39;

   public static final int removeKeyVersionedMethodLocal_id = 40;

   public static final int removeDataVersionedMethodLocal_id = 41;


    static
    {
        try
        {
            getDataMapMethodLocal = TreeCache.class.getDeclaredMethod("_getData", new Class[]
                    {Fqn.class});
            existsMethod = TreeCache.class.getDeclaredMethod("exists", new Class[]
                    {Fqn.class});
            putDataMethodLocal = TreeCache.class.getDeclaredMethod("_put", new Class[]
                    {GlobalTransaction.class, Fqn.class, Map.class, boolean.class});
            putDataEraseMethodLocal = TreeCache.class.getDeclaredMethod("_put", new Class[]
                    {GlobalTransaction.class, Fqn.class, Map.class, boolean.class, boolean.class});
            putKeyValMethodLocal = TreeCache.class.getDeclaredMethod("_put", new Class[]
                    {GlobalTransaction.class, Fqn.class, Object.class, Object.class, boolean.class});
            putFailFastKeyValueMethodLocal = TreeCache.class.getDeclaredMethod("_put", new Class[]
                    {GlobalTransaction.class, Fqn.class, Object.class, Object.class, boolean.class, long.class});
            removeNodeMethodLocal = TreeCache.class.getDeclaredMethod("_remove", new Class[]
                    {GlobalTransaction.class, Fqn.class, boolean.class});
            removeKeyMethodLocal = TreeCache.class.getDeclaredMethod("_remove", new Class[]
                    {GlobalTransaction.class, Fqn.class, Object.class, boolean.class});
            removeDataMethodLocal = TreeCache.class.getDeclaredMethod("_removeData", new Class[]
                    {GlobalTransaction.class, Fqn.class, boolean.class});
            evictNodeMethodLocal = TreeCache.class.getDeclaredMethod("_evict", new Class[]
                    {Fqn.class});
            evictVersionedNodeMethodLocal = TreeCache.class.getDeclaredMethod("_evict", new Class[]
                    {Fqn.class, DataVersion.class});

            // evictKeyValueMethodLocal=TreeCache.class.getDeclaredMethod("_evict", new Class[]{Fqn.class, Object.class});
            prepareMethod = TreeCache.class.getDeclaredMethod("prepare", new Class[]
                    {GlobalTransaction.class, List.class, Address.class, boolean.class});
            commitMethod = TreeCache.class.getDeclaredMethod("commit", new Class[]
                    {GlobalTransaction.class});
            rollbackMethod = TreeCache.class.getDeclaredMethod("rollback", new Class[]
                    {GlobalTransaction.class});
            addChildMethodLocal = TreeCache.class.getDeclaredMethod("_addChild", new Class[]
                    {GlobalTransaction.class, Fqn.class, Object.class, DataNode.class});
            getKeyValueMethodLocal = TreeCache.class.getDeclaredMethod("_get", new Class[]
                    {Fqn.class, Object.class, boolean.class});
            getNodeMethodLocal = TreeCache.class.getDeclaredMethod("_get", new Class[]
                    {Fqn.class});
            getKeysMethodLocal = TreeCache.class.getDeclaredMethod("_getKeys", new Class[]
                    {Fqn.class});
            getChildrenNamesMethodLocal = TreeCache.class.getDeclaredMethod("_getChildrenNames", new Class[]
                    {Fqn.class});
            replicateMethod = TreeCache.class.getDeclaredMethod("_replicate", new Class[]
                    {MethodCall.class});
            replicateAllMethod = TreeCache.class.getDeclaredMethod("_replicate", new Class[]
                    {List.class});
            releaseAllLocksMethodLocal = TreeCache.class.getDeclaredMethod("_releaseAllLocks", new Class[]
                    {Fqn.class});
            printMethodLocal = TreeCache.class.getDeclaredMethod("_print", new Class[]
                    {Fqn.class});
            lockMethodLocal = TreeCache.class.getDeclaredMethod("_lock", new Class[]
                    {Fqn.class, int.class, boolean.class});

            optimisticPrepareMethod = TreeCache.class.getDeclaredMethod("optimisticPrepare", new Class[]
                    {GlobalTransaction.class, List.class, Map.class, Address.class, boolean.class});

            getPartialStateMethod = TreeCache.class.getDeclaredMethod("_getState", new Class[]
                    {Fqn.class, long.class, boolean.class, boolean.class});

            clusteredGetMethod = TreeCache.class.getDeclaredMethod("_clusteredGet", new Class[]
                    {MethodCall.class, Boolean.class});

            enqueueMethodCallMethod = TreeCache.class.getDeclaredMethod("_enqueueMethodCall", new Class[]
                    {String.class, MethodCall.class});

            notifyCallOnInactiveMethod = TreeCache.class.getDeclaredMethod("notifyCallForInactiveSubtree", new Class[]
                    {String.class});

            // ------------ buddy replication

            remoteAnnounceBuddyPoolNameMethod = TreeCache.class.getDeclaredMethod("_remoteAnnounceBuddyPoolName",
                    new Class[]
                            {IpAddress.class, String.class});
            remoteRemoveFromBuddyGroupMethod = TreeCache.class.getDeclaredMethod("_remoteRemoveFromBuddyGroup",
                    new Class[]
                            {String.class});
            remoteAssignToBuddyGroupMethod = TreeCache.class.getDeclaredMethod("_remoteAssignToBuddyGroup", new Class[]
                    {BuddyGroup.class, Map.class});

            dataGravitationCleanupMethod = TreeCache.class.getDeclaredMethod("_dataGravitationCleanup", new Class[]{GlobalTransaction.class, Fqn.class, Fqn.class});
            dataGravitationMethod = TreeCache.class.getDeclaredMethod("_gravitateData", new Class[]{Fqn.class, boolean.class, boolean.class});

           // version-aware methods - see JBCACHE-843
            putDataVersionedMethodLocal = TreeCache.class.getDeclaredMethod("_put", new Class[]
                    {GlobalTransaction.class, Fqn.class, Map.class, boolean.class, DataVersion.class});
            putDataEraseVersionedMethodLocal = TreeCache.class.getDeclaredMethod("_put", new Class[]
                    {GlobalTransaction.class, Fqn.class, Map.class, boolean.class, boolean.class, DataVersion.class});
            putKeyValVersionedMethodLocal = TreeCache.class.getDeclaredMethod("_put", new Class[]
                    {GlobalTransaction.class, Fqn.class, Object.class, Object.class, boolean.class, DataVersion.class});
            removeNodeVersionedMethodLocal = TreeCache.class.getDeclaredMethod("_remove", new Class[]
                    {GlobalTransaction.class, Fqn.class, boolean.class, DataVersion.class});
            removeKeyVersionedMethodLocal = TreeCache.class.getDeclaredMethod("_remove", new Class[]
                    {GlobalTransaction.class, Fqn.class, Object.class, boolean.class, DataVersion.class});
            removeDataVersionedMethodLocal = TreeCache.class.getDeclaredMethod("_removeData", new Class[]
                    {GlobalTransaction.class, Fqn.class, boolean.class, DataVersion.class});
        }
        catch (NoSuchMethodException ex)
        {
            ex.printStackTrace();
            throw new ExceptionInInitializerError(ex.toString());
        }

        methods.put(new Integer(putDataMethodLocal_id), putDataMethodLocal);
        methods.put(new Integer(putDataEraseMethodLocal_id), putDataEraseMethodLocal);
        methods.put(new Integer(putKeyValMethodLocal_id), putKeyValMethodLocal);
        methods.put(new Integer(putFailFastKeyValueMethodLocal_id), putFailFastKeyValueMethodLocal);
        methods.put(new Integer(removeNodeMethodLocal_id), removeNodeMethodLocal);
        methods.put(new Integer(removeKeyMethodLocal_id), removeKeyMethodLocal);
        methods.put(new Integer(removeDataMethodLocal_id), removeDataMethodLocal);
        methods.put(new Integer(evictNodeMethodLocal_id), evictNodeMethodLocal);
        methods.put(new Integer(evictVersionedNodeMethodLocal_id), evictVersionedNodeMethodLocal);
        methods.put(new Integer(prepareMethod_id), prepareMethod);
        methods.put(new Integer(commitMethod_id), commitMethod);
        methods.put(new Integer(rollbackMethod_id), rollbackMethod);
        methods.put(new Integer(replicateMethod_id), replicateMethod);
        methods.put(new Integer(replicateAllMethod_id), replicateAllMethod);
        methods.put(new Integer(addChildMethodLocal_id), addChildMethodLocal);
        methods.put(new Integer(existsMethod_id), existsMethod);
        methods.put(new Integer(releaseAllLocksMethodLocal_id), releaseAllLocksMethodLocal);
        methods.put(new Integer(optimisticPrepareMethod_id), optimisticPrepareMethod);
        methods.put(new Integer(getPartialStateMethod_id), getPartialStateMethod);
        methods.put(new Integer(enqueueMethodCallMethod_id), enqueueMethodCallMethod);
        methods.put(new Integer(notifyCallOnInactiveMethod_id), notifyCallOnInactiveMethod);
        methods.put(new Integer(clusteredGetMethod_id), clusteredGetMethod);
        methods.put(new Integer(getChildrenNamesMethodLocal_id), getChildrenNamesMethodLocal);
        methods.put(new Integer(getDataMapMethodLocal_id), getDataMapMethodLocal);
        methods.put(new Integer(getKeysMethodLocal_id), getKeysMethodLocal);
        methods.put(new Integer(getKeyValueMethodLocal_id), getKeyValueMethodLocal);
        methods.put(new Integer(dispatchRpcCallMethod_id), RpcTreeCache.dispatchRpcCallMethod);
        methods.put(new Integer(remoteAnnounceBuddyPoolNameMethod_id), remoteAnnounceBuddyPoolNameMethod);
        methods.put(new Integer(remoteAssignToBuddyGroupMethod_id), remoteAssignToBuddyGroupMethod);
        methods.put(new Integer(remoteRemoveFromBuddyGroupMethod_id), remoteRemoveFromBuddyGroupMethod);
        /* Mappings added as they did not exist before refactoring */
        methods.put(new Integer(getNodeMethodLocal_id), getNodeMethodLocal);
        methods.put(new Integer(printMethodLocal_id), printMethodLocal);
        methods.put(new Integer(lockMethodLocal_id), lockMethodLocal);

        methods.put(new Integer(dataGravitationCleanupMethod_id), dataGravitationCleanupMethod);
        methods.put(new Integer(dataGravitationMethod_id), dataGravitationMethod);

       methods.put(new Integer(putDataVersionedMethodLocal_id), putDataVersionedMethodLocal);
       methods.put(new Integer(putDataEraseVersionedMethodLocal_id), putDataEraseVersionedMethodLocal);
       methods.put(new Integer(putKeyValVersionedMethodLocal_id), putKeyValVersionedMethodLocal);
       methods.put(new Integer(removeDataVersionedMethodLocal_id), removeDataVersionedMethodLocal);
       methods.put(new Integer(removeKeyVersionedMethodLocal_id), removeKeyVersionedMethodLocal);
       methods.put(new Integer(removeNodeVersionedMethodLocal_id), removeNodeVersionedMethodLocal);

        Iterator it = methods.keySet().iterator();
        while (it.hasNext())
        {
            Object id = it.next();
            Object method = methods.get(id);
            methodIds.put(method, id);
        }

        crud_method_ids.add(new Integer(putDataMethodLocal_id));
        crud_method_ids.add(new Integer(putDataEraseMethodLocal_id));
        crud_method_ids.add(new Integer(putKeyValMethodLocal_id));
        crud_method_ids.add(new Integer(putFailFastKeyValueMethodLocal_id));
        crud_method_ids.add(new Integer(removeNodeMethodLocal_id));
        crud_method_ids.add(new Integer(removeKeyMethodLocal_id));
        crud_method_ids.add(new Integer(removeDataMethodLocal_id));
        crud_method_ids.add(new Integer(dataGravitationCleanupMethod_id));
        crud_method_ids.add(new Integer(putDataVersionedMethodLocal_id));
        crud_method_ids.add(new Integer(putDataEraseVersionedMethodLocal_id));
        crud_method_ids.add(new Integer(putKeyValVersionedMethodLocal_id));
        crud_method_ids.add(new Integer(removeNodeVersionedMethodLocal_id));
        crud_method_ids.add(new Integer(removeKeyVersionedMethodLocal_id));
        crud_method_ids.add(new Integer(removeDataVersionedMethodLocal_id));

       crud_methods.add(putDataMethodLocal);
       crud_methods.add(putDataEraseMethodLocal);
       crud_methods.add(putKeyValMethodLocal);
       crud_methods.add(putFailFastKeyValueMethodLocal);
       crud_methods.add(removeNodeMethodLocal);
       crud_methods.add(removeKeyMethodLocal);
       crud_methods.add(removeDataMethodLocal);
       crud_methods.add(dataGravitationCleanupMethod);
      crud_methods.add(putDataVersionedMethodLocal);
      crud_methods.add(putDataEraseVersionedMethodLocal);
      crud_methods.add(putKeyValVersionedMethodLocal);
      crud_methods.add(removeDataVersionedMethodLocal);
      crud_methods.add(removeNodeVersionedMethodLocal);
      crud_methods.add(removeKeyVersionedMethodLocal);

       optimisticPutMethods.add(putDataEraseMethodLocal);
       optimisticPutMethods.add(putDataMethodLocal);
       optimisticPutMethods.add(putKeyValMethodLocal);
       optimisticPutMethods.add(putDataEraseVersionedMethodLocal);
       optimisticPutMethods.add(putDataVersionedMethodLocal);
       optimisticPutMethods.add(putKeyValVersionedMethodLocal);


    }

    protected static int lookupMethodId(Method method)
    {
        Integer methodIdInteger = (Integer) methodIds.get(method);
        int methodId = -1;

        if (methodIdInteger != null)
        {
            methodId = methodIdInteger.intValue();
        }
        else
        {
            if (log.isWarnEnabled())
            {
                log.warn("Method " + method + " is not registered with " + TreeCacheMarshaller140.class);
            }
        }

        return methodId;
    }

    protected static Method lookupMethod(int id)
    {
        Method method = (Method) methods.get(new Integer(id));
        if (method == null)
        {
            if (log.isErrorEnabled())
            {
                log.error("Method id " + id + " is not registered");
            }
            throw new RuntimeException("Method id " + id + " is not registered with " + TreeCacheMarshaller140.class);
        }
        return method;
    }

    /**
     * Returns true if the method is a CRUD method.
     */
    public static boolean isCrudMethod(Method m)
    {
        return crud_methods.contains(m);
    }

    public static boolean isCrudMethod(int id)
    {
        return crud_method_ids.contains(new Integer(id));
    }

   /**
    * Returns the versioned equivalent of a crud method.
    */
   public static Method getVersionedMethod(int methodId)
   {
      if (isCrudMethod(methodId))
      {
         switch (methodId)
         {
            case putDataEraseMethodLocal_id:
               return putDataEraseVersionedMethodLocal;
            case putDataMethodLocal_id:
               return putDataVersionedMethodLocal;
            case putKeyValMethodLocal_id:
               return putKeyValVersionedMethodLocal;
            case removeDataMethodLocal_id:
               return removeDataVersionedMethodLocal;
            case removeKeyMethodLocal_id:
               return removeKeyVersionedMethodLocal;
            case removeNodeMethodLocal_id:
               return removeNodeVersionedMethodLocal;
            default:
               throw new RuntimeException("Unrecognised method id " + methodId);
         }
      }
      else throw new RuntimeException("Attempting to look up a versioned equivalent of a non-crud method");
   }

   /**
    * Returns true if the method passed in is one of the put or put-with-versioning methods.
    */
   public static boolean isOptimisticPutMethod(Method method)
   {
      return optimisticPutMethods.contains(method);
   }

   /**
    * Counterpart to {@link #getVersionedMethod(int)}
    */
   public static Method getUnversionedMethod(int methodId)
   {
      if (isCrudMethod(methodId))
      {
         switch (methodId)
         {
            case putDataEraseVersionedMethodLocal_id:
               return putDataEraseMethodLocal;
            case putDataVersionedMethodLocal_id:
               return putDataMethodLocal;
            case putKeyValVersionedMethodLocal_id:
               return putKeyValMethodLocal;
            case removeDataVersionedMethodLocal_id:
               return removeDataMethodLocal;
            case removeKeyVersionedMethodLocal_id:
               return removeKeyMethodLocal;
            case removeNodeVersionedMethodLocal_id:
               return removeNodeMethodLocal;
            default:
               throw new RuntimeException("Unrecognised method id " + methodId);
         }
      }
      else throw new RuntimeException("Attempting to look up a versioned equivalent of a non-crud method");
   }

   public static boolean isDataGravitationMethod(int methodId)
   {
      return methodId == MethodDeclarations.dataGravitationCleanupMethod_id || methodId == MethodDeclarations.dataGravitationMethod_id;
   }

   public static boolean isGetMethod(int methodId)
   {
      return methodId == getChildrenNamesMethodLocal_id || methodId == getDataMapMethodLocal_id || methodId == existsMethod_id
              || methodId == getKeysMethodLocal_id || methodId == getKeyValueMethodLocal_id || methodId == getNodeMethodLocal_id;
   }
}
