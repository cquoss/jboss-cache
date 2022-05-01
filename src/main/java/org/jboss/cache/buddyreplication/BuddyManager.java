/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.buddyreplication;

import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeCacheListener;
import org.jboss.cache.lock.TimeoutException;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.Region;
import org.jboss.cache.marshall.RegionManager;
import org.jboss.cache.marshall.VersionAwareMarshaller;
import org.jboss.cache.xml.XmlHelper;
import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.stack.IpAddress;
import org.w3c.dom.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Class that manages buddy replication groups.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class BuddyManager
{
   private static Log log = LogFactory.getLog(BuddyManager.class);

   /**
    * Test whether buddy replication is enabled.
    */
   private boolean enabled;

   /**
    * Buddy locator class
    */
   BuddyLocator buddyLocator;

   /**
    * back-refernce to the TreeCache object
    */
   private TreeCache cache;

   /**
    * The buddy group set up for this instance
    */
   BuddyGroup buddyGroup;

   /**
    * Map of buddy pools received from broadcasts
    */
   Map buddyPool = new ConcurrentReaderHashMap();
   /**
    * The nullBuddyPool is a set of addresses that have not specified buddy pools.
    */
   final Set nullBuddyPool = new HashSet();
   /**
    * Name of the buddy pool for current instance.  May be null if buddy pooling is not used.
    */
   String buddyPoolName;

   boolean autoDataGravitation = true;
   boolean dataGravitationRemoveOnFind = true;
   boolean dataGravitationSearchBackupTrees = true;

   /**
    * Map of bddy groups the current instance participates in as a backup node.
    * Keyed on String group name, values are BuddyGroup objects.
    * Needs to deal with concurrent access - concurrent assignTo/removeFrom buddy grp
    */
   Map buddyGroupsIParticipateIn = new ConcurrentReaderHashMap();

   /**
    * Queue to deal with queued up view change requests - which are handled asynchronously
    */
   private final BoundedLinkedQueue queue = new BoundedLinkedQueue();

   /**
    * Async thread that handles items on the view change queue
    */
   private AsyncViewChangeHandlerThread asyncViewChangeHandler = new AsyncViewChangeHandlerThread();
   private static SynchronizedInt threadId = new SynchronizedInt(0);

   /**
    * Constants representng the buddy backup subtree
    */
   public static final String BUDDY_BACKUP_SUBTREE = "_BUDDY_BACKUP_";
   public static final Fqn BUDDY_BACKUP_SUBTREE_FQN = Fqn.fromString(BUDDY_BACKUP_SUBTREE);

   int buddyCommunicationTimeout = 10000;
   /**
    * number of times to retry communicating with a selected buddy if the buddy has not been initialised.
    */
   private static int UNINIT_BUDDIES_RETRIES = 3;
   /**
    * wait time between retries
    */
   private static final long UNINIT_BUDDIES_RETRY_NAPTIME = 500;

   /**
    * Flag to prevent us receiving and processing remote calls before we've started
    */
   private boolean initialised = false;

   /**
    * Lock to synchronise on to ensure buddy pool info is received before buddies are assigned to groups.
    */
   private final Object poolInfoNotifierLock = new Object();

   public BuddyManager(Element element)
   {
      enabled = XmlHelper.readBooleanContents(element, "buddyReplicationEnabled");
      dataGravitationRemoveOnFind = XmlHelper.readBooleanContents(element, "dataGravitationRemoveOnFind", true);
      dataGravitationSearchBackupTrees = XmlHelper.readBooleanContents(element, "dataGravitationSearchBackupTrees", true);
      autoDataGravitation = enabled && XmlHelper.readBooleanContents(element, "autoDataGravitation", false);

      String strBuddyCommunicationTimeout = XmlHelper.readStringContents(element, "buddyCommunicationTimeout");
      try
      {
         buddyCommunicationTimeout = Integer.parseInt(strBuddyCommunicationTimeout);
      }
      catch (Exception e)
      {
      }
      finally
      {
         if (log.isDebugEnabled())
            log.debug("Using buddy communication timeout of " + buddyCommunicationTimeout + " millis");
      }
      buddyPoolName = XmlHelper.readStringContents(element, "buddyPoolName");
      if (buddyPoolName != null && buddyPoolName.equals("")) buddyPoolName = null;

      // now read the buddy locator details and create accordingly.
      String buddyLocatorClass = null;
      Properties buddyLocatorProperties = null;
      try
      {
         buddyLocatorClass = XmlHelper.readStringContents(element, "buddyLocatorClass");
         try
         {
            buddyLocatorProperties = XmlHelper.readPropertiesContents(element, "buddyLocatorProperties");
         }
         catch (IOException e)
         {
            log.warn("Caught exception reading buddyLocatorProperties", e);
            log.error("Unable to read buddyLocatorProperties specified!  Using defaults for [" + buddyLocatorClass + "]");
         }

         // its OK if the buddy locator class or properties are null.
         buddyLocator = (buddyLocatorClass == null || buddyLocatorClass.equals("")) ? createDefaultBuddyLocator(buddyLocatorProperties) : createBuddyLocator(buddyLocatorClass, buddyLocatorProperties);
      }
      catch (Exception e)
      {
         log.warn("Caught exception instantiating buddy locator", e);
         log.error("Unable to instantiate specified buddyLocatorClass [" + buddyLocatorClass + "].  Using default buddyLocator [" + NextMemberBuddyLocator.class.getName() + "] instead, with default properties.");
         buddyLocator = createDefaultBuddyLocator(null);
      }
   }

   protected BuddyLocator createBuddyLocator(String className, Properties props) throws ClassNotFoundException, IllegalAccessException, InstantiationException
   {
      BuddyLocator bl = (BuddyLocator) Class.forName(className).newInstance();
      bl.init(props);
      return bl;
   }

   protected BuddyLocator createDefaultBuddyLocator(Properties props)
   {
      BuddyLocator bl = new NextMemberBuddyLocator();
      bl.init(props);
      return bl;
   }

   public boolean isEnabled()
   {
      return enabled;
   }

   public String getBuddyPoolName()
   {
      return buddyPoolName;
   }

   public static String getGroupNameFromAddress(Object address)
   {
      String s = address.toString();
      return s.replace(':', '_');
   }

   public void init(TreeCache cache) throws Exception
   {
      this.cache = cache;
      final IpAddress localAddress = (IpAddress) cache.getLocalAddress();
      buddyGroup = new BuddyGroup();
      buddyGroup.setDataOwner(localAddress);
      buddyGroup.setGroupName(getGroupNameFromAddress(localAddress));
      log.debug("Starting buddy manager for data owner " + buddyGroup.getDataOwner());


      if (buddyPoolName != null)
      {
         buddyPool.put(buddyGroup.getDataOwner(), buddyPoolName);
      }

      broadcastBuddyPoolMembership();

      // allow waiting threads to process.
      initialised = true;

      // register a TreeCache Listener to reassign buddies as and when view changes occur

      cache.addTreeCacheListener(new TreeCacheListener()
      {
         private Vector oldMembers;

         public void nodeCreated(Fqn fqn)
         {
         }

         public void nodeRemoved(Fqn fqn)
         {
         }

         public void nodeLoaded(Fqn fqn)
         {
         }

         public void nodeEvicted(Fqn fqn)
         {
         }

         public void nodeModified(Fqn fqn)
         {
         }

         public void nodeVisited(Fqn fqn)
         {
         }

         public void cacheStarted(TreeCache cache)
         {
         }

         public void cacheStopped(TreeCache cache)
         {
         }

         public void viewChange(View newView)
         {
            Vector newMembers = newView.getMembers();

            enqueueViewChange(oldMembers == null ? null : new Vector(oldMembers), new Vector(newMembers));
            if (oldMembers == null) oldMembers = new Vector();
            oldMembers.clear();
            oldMembers.addAll(newMembers);
         }
      });

      // assign buddies based on what we know now
      reassignBuddies(cache.getMembers());
      asyncViewChangeHandler.start();
   }

   public boolean isAutoDataGravitation()
   {
      return autoDataGravitation;
   }

   public boolean isDataGravitationRemoveOnFind()
   {
      return dataGravitationRemoveOnFind;
   }

   public boolean isDataGravitationSearchBackupTrees()
   {
      return dataGravitationSearchBackupTrees;
   }

   public int getBuddyCommunicationTimeout()
   {
      return buddyCommunicationTimeout;
   }

   // -------------- methods to be called by the tree cache listener --------------------

   private void enqueueViewChange(List oldMembers, List newMembers)
   {
      // put this on a queue
      try
      {
         queue.put(new List[]{oldMembers, newMembers});
      }
      catch (InterruptedException e)
      {
         log.warn("Caught interrupted exception trying to enqueue a view change event", e);
      }
   }

   /**
    * Called by the TreeCacheListener when a
    * view change is detected.  Used to find new buddies if
    * existing buddies have died or if new members to the cluster
    * have been added.  Makes use of the BuddyLocator and then
    * makes RPC calls to remote nodes to assign/remove buddies.
    */
   private void reassignBuddies(List membership) throws Exception
   {
      if (log.isDebugEnabled())
      {
         log.debug("Data owner address " + cache.getLocalAddress());
         log.debug("Entering updateGroup.  Current group: " + buddyGroup + ".  Current View membership: " + membership);
      }
      // some of my buddies have died!
      List newBuddies = buddyLocator.locateBuddies(buddyPool, membership, buddyGroup.getDataOwner());
      List uninitialisedBuddies = new ArrayList();
      Iterator newBuddiesIt = newBuddies.iterator();
      while (newBuddiesIt.hasNext())
      {
         Object newBuddy = newBuddiesIt.next();
         if (!buddyGroup.buddies.contains(newBuddy))
         {
            uninitialisedBuddies.add(newBuddy);
         }
      }

      List obsoleteBuddies = new ArrayList();
      // find obsolete buddies
      Iterator originalBuddies = buddyGroup.buddies.iterator();
      while (originalBuddies.hasNext())
      {
         Object origBuddy = originalBuddies.next();
         if (!newBuddies.contains(origBuddy))
         {
            obsoleteBuddies.add(origBuddy);
         }
      }

      // Update buddy list
      if (!obsoleteBuddies.isEmpty())
      {
         removeFromGroup(obsoleteBuddies);
      }
      else
      {
         log.trace("No obsolete buddies found, nothing to announce.");
      }
      if (!uninitialisedBuddies.isEmpty())
      {
         addBuddies(uninitialisedBuddies);
      }
      else
      {
         log.trace("No uninitialized buddies found, nothing to announce.");
      }

      log.info("New buddy group: " + buddyGroup);
   }

   // -------------- methods to be called by the tree cache  --------------------

   /**
    * Called by TreeCache._remoteAnnounceBuddyPoolName(Address address, String buddyPoolName)
    * when a view change occurs and caches need to inform the cluster of which buddy pool it is in.
    */
   public void handlePoolNameBroadcast(IpAddress address, String poolName)
   {
      if (log.isDebugEnabled())
      {
         log.debug("BuddyManager@" + Integer.toHexString(hashCode()) + ": received announcement that cache instance " + address + " is in buddy pool " + poolName);
      }
      if (poolName != null)
      {
            buddyPool.put(address, poolName);
      }
      else
      {
         synchronized(nullBuddyPool)
         {
            // writes to this concurrent set are expensive.  Don't write unnecessarily.
            if (!nullBuddyPool.contains(address)) nullBuddyPool.add(address);
         }
      }

      // notify any waiting view change threads that buddy pool info has been received.
      synchronized (poolInfoNotifierLock)
      {
         log.trace("Notifying any waiting view change threads that we have received buddy pool info.");
         poolInfoNotifierLock.notifyAll();
      }
   }

   /**
    * Called by TreeCache._remoteRemoveFromBuddyGroup(String groupName)
    * when a method call for this is received from a remote cache.
    */
   public void handleRemoveFromBuddyGroup(String groupName) throws BuddyNotInitException
   {
      if (!initialised) throw new BuddyNotInitException("Not yet initialised");
      if (log.isInfoEnabled()) log.info("Removing self from buddy group " + groupName);
      buddyGroupsIParticipateIn.remove(groupName);

      // remove backup data for this group
      if (log.isInfoEnabled()) log.info("Removing backup data for group " + groupName);
      try
      {
         cache.remove(new Fqn(BUDDY_BACKUP_SUBTREE_FQN, groupName));
      }
      catch (CacheException e)
      {
         log.error("Unable to remove backup data for group " + groupName, e);
      }
   }

   /**
    * Called by TreeCache._remoteAssignToBuddyGroup(BuddyGroup g) when a method
    * call for this is received from a remote cache.
    *
    * @param newGroup the buddy group
    * @param state    Map<Fqn, byte[]> of any state from the DataOwner. Cannot
    *                 be <code>null</code>.
    */
   public void handleAssignToBuddyGroup(BuddyGroup newGroup, Map state) throws Exception
   {
      if (log.isTraceEnabled())
         log.trace("Handling assign to buddy grp.  Sender: " + newGroup.getGroupName() + "; My instance: " + buddyGroup.getDataOwner());
      // if we haven't initialised, throw an exception.
      if (!initialised) throw new BuddyNotInitException("Not yet initialised");

      if (log.isInfoEnabled()) log.info("Assigning self to buddy group " + newGroup);
      buddyGroupsIParticipateIn.put(newGroup.getGroupName(), newGroup);

      // Integrate state transfer from the data owner of the buddy group
      Fqn integrationBase = new Fqn(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN,
              newGroup.getGroupName());
      VersionAwareMarshaller marshaller = null;
      if (cache.getUseRegionBasedMarshalling())
      {
         marshaller = cache.getMarshaller();
      }

      for (Iterator it = state.entrySet().iterator(); it.hasNext();)
      {
         Map.Entry entry = (Map.Entry) it.next();
         Fqn fqn = (Fqn) entry.getKey();
         String fqnS = fqn.toString();
         if (marshaller == null || !marshaller.isInactive(fqn.toString()))
         {
            ClassLoader cl = (marshaller == null) ? null : marshaller.getClassLoader(fqnS);
            Fqn integrationRoot = new Fqn(integrationBase, fqn);
            cache._setState((byte[]) entry.getValue(), integrationRoot, cl);
         }
      }
   }
   
   /**
    * Returns a List<IpAddress> identifying the DataOwner for each buddy
    * group for which this node serves as a backup node.
    */
   public List getBackupDataOwners()
   {
      List owners = new ArrayList();
      for (Iterator it = buddyGroupsIParticipateIn.values().iterator(); it.hasNext();)
      {
         BuddyGroup group = (BuddyGroup) it.next();
         owners.add(group.getDataOwner());
      }
      return owners;
   }

   // -------------- static util methods ------------------

   public static Fqn getBackupFqn(Object dataOwnerAddress, Fqn origFqn)
   {
      return getBackupFqn(getGroupNameFromAddress(dataOwnerAddress), origFqn);
   }

   public static Fqn getBackupFqn(String buddyGroupName, Fqn origFqn)
   {
      if (isBackupFqn(origFqn)) throw new RuntimeException("Cannot make a backup Fqn from a backup Fqn! Attempting to create a backup of " + origFqn);
      List elements = new ArrayList();
      elements.add(BUDDY_BACKUP_SUBTREE);
      elements.add(buddyGroupName);
      elements.addAll(origFqn.peekElements());

      return new Fqn(elements);
   }
                                                                                 
   public static Fqn getBackupFqn(Fqn buddyGroupRoot, Fqn origFqn)
   {
      if (origFqn.isChildOf(buddyGroupRoot))
      {
         return origFqn;
      }

      List elements = new ArrayList();
      elements.add(BUDDY_BACKUP_SUBTREE);
      elements.add(buddyGroupRoot.get(1));
      elements.addAll(origFqn.peekElements());

      return new Fqn(elements);
   }

   public static boolean isBackupFqn(Fqn name)
   {
      return name != null && name.hasElement(BuddyManager.BUDDY_BACKUP_SUBTREE);
   }

   // -------------- methods to be called by the BaseRPCINterceptor --------------------

   /**
    * Returns a list of buddies for which this instance is Data Owner.
    * List excludes self.  Used by the BaseRPCInterceptor when deciding
    * who to replicate to.
    */
   public List getBuddyAddresses()
   {
      return buddyGroup.buddies;
   }

   /**
    * Introspects method call for Fqns and changes them such that they
    * are under the current buddy group's backup subtree
    * (e.g., /_buddy_backup_/my_host:7890/) rather than the root (/).
    * Called by BaseRPCInterceptor to transform method calls before broadcasting.
    */
   public JBCMethodCall transformFqns(JBCMethodCall call)
   {
      return transformFqns(call, call.getMethodId() != MethodDeclarations.dataGravitationCleanupMethod_id);
   }

   public JBCMethodCall transformFqns(JBCMethodCall call, boolean transformForCurrentCall)
   {
      if (call != null && call.getArgs() != null)
      {
         JBCMethodCall call2 = new JBCMethodCall(call.getMethod(), (Object[]) call.getArgs().clone(), call.getMethodId());
         handleArgs(call2.getArgs(), transformForCurrentCall);
         return call2;
      }
      else
      {
         return call;
      }
   }

   // -------------- internal helpers methods --------------------

   private void removeFromGroup(List buddies) throws Exception
   {
      if (log.isInfoEnabled())
      {
         log.info("Removing obsolete buddies from buddy group [" + buddyGroup.getGroupName() + "].  Obsolete buddies are " + buddies);
      }
      buddyGroup.buddies.removeAll(buddies);
      // now broadcast a message to the removed buddies.
      MethodCall membershipCall = MethodCallFactory.create(MethodDeclarations.remoteRemoveFromBuddyGroupMethod, new Object[]{buddyGroup.getGroupName()});
      MethodCall replicateCall = MethodCallFactory.create(MethodDeclarations.replicateMethod, new Object[]{membershipCall});


      int attemptsLeft = UNINIT_BUDDIES_RETRIES;

      while (attemptsLeft-- > 0)
      {
         try
         {
            makeRemoteCall(buddies, replicateCall, true);
            break;
         }
         catch (Exception e)
         {
            if (e instanceof BuddyNotInitException || e.getCause() instanceof BuddyNotInitException)
            {
               if (attemptsLeft > 0)
               {
                  log.info("One of the buddies have not been initialised.  Will retry after a short nap.");
                  Thread.sleep(UNINIT_BUDDIES_RETRY_NAPTIME);
               }
               else
               {
                  throw new BuddyNotInitException("Unable to contact buddy after " + UNINIT_BUDDIES_RETRIES + " retries");
               }
            }
            else
            {
               log.error("Unable to communicate with Buddy for some reason", e);
            }
         }
      }

      log.trace("removeFromGroup notification complete");
   }

   private void addBuddies(List buddies) throws Exception
   {
      if (log.isInfoEnabled())
      {
         log.info("Assigning new buddies to buddy group [" + buddyGroup.getGroupName() + "].  New buddies are " + buddies);
      }

      buddyGroup.buddies.addAll(buddies);

      // Create the state transfer map

      Map stateMap = new HashMap();
      byte[] state = null;
      if (cache.getUseRegionBasedMarshalling())
      {
         RegionManager rm = cache.getRegionManager();
         Region[] regions = rm.getRegions();
         if (regions.length > 0)
         {
            for (int i = 0; i < regions.length; i++)
            {
               Fqn f = Fqn.fromString(regions[i].getFqn());
               state = acquireState(f);
               if (state != null)
               {
                  stateMap.put(f, state);
               }
            }
         }
         else if (!cache.isInactiveOnStartup())
         {
            // No regions defined; try the root
            state = acquireState(Fqn.ROOT);
            if (state != null)
            {
               stateMap.put(Fqn.ROOT, state);
            }
         }
      }
      else
      {
         state = acquireState(Fqn.ROOT);
         if (state != null)
         {
            stateMap.put(Fqn.ROOT, state);
         }
      }

      // now broadcast a message to the newly assigned buddies.
      MethodCall membershipCall = MethodCallFactory.create(MethodDeclarations.remoteAssignToBuddyGroupMethod, new Object[]{buddyGroup, stateMap});
      MethodCall replicateCall = MethodCallFactory.create(MethodDeclarations.replicateMethod, new Object[]{membershipCall});

      int attemptsLeft = UNINIT_BUDDIES_RETRIES;

      while (attemptsLeft-- > 0)
      {
         try
         {
            makeRemoteCall(buddies, replicateCall, true);
            break;
         }
         catch (Exception e)
         {
            if (e instanceof BuddyNotInitException || e.getCause() instanceof BuddyNotInitException)
            {
               if (attemptsLeft > 0)
               {
                  log.info("One of the buddies have not been initialised.  Will retry after a short nap.");
                  Thread.sleep(UNINIT_BUDDIES_RETRY_NAPTIME);
               }
               else
               {
                  throw new BuddyNotInitException("Unable to contact buddy after " + UNINIT_BUDDIES_RETRIES + " retries");
               }
            }
            else
            {
               log.error("Unable to communicate with Buddy for some reason", e);
            }
         }
      }


      if (log.isTraceEnabled())
         log.trace("addToGroup notification complete (data owner " + buddyGroup.getDataOwner() + ")");
   }

   private byte[] acquireState(Fqn fqn) throws Exception
   {
      // Call _getState with progressively longer timeouts until we
      // get state or it doesn't throw a TimeoutException
      long[] timeouts = {400, 800, 1600};
      TimeoutException timeoutException = null;

      boolean trace = log.isTraceEnabled();

      for (int i = 0; i < timeouts.length; i++)
      {
         timeoutException = null;

         boolean force = (i == timeouts.length - 1);

         try
         {
            byte[] state = cache._getState(fqn, cache.getFetchInMemoryState(),
                    cache.getFetchPersistentState(), timeouts[i], force, false);
            if (log.isDebugEnabled())
            {
               log.debug("acquireState(): got state");
            }
            return state;
         }
         catch (TimeoutException t)
         {
            timeoutException = t;
            if (trace)
            {
               log.trace("acquireState(): got a TimeoutException");
            }
         }
         catch (Exception e)
         {
            throw e;
         }
         catch (Throwable t)
         {
            throw new RuntimeException(t);
         }
      }

      // If we got a timeout exception on the final try,
      // this is a failure condition
      if (timeoutException != null)
      {
         throw new CacheException("acquireState(): Failed getting state due to timeout",
                 timeoutException);
      }

      if (log.isDebugEnabled())
      {
         log.debug("acquireState(): Unable to give state");
      }

      return null;
   }

   /**
    * Called by the BuddyGroupMembershipMonitor every time a view change occurs.
    */
   private void broadcastBuddyPoolMembership()
   {
      broadcastBuddyPoolMembership(null);
   }

   private void broadcastBuddyPoolMembership(List recipients)
   {
      // broadcast to other caches
      if (log.isDebugEnabled())
      {
         log.debug("Instance " + buddyGroup.getDataOwner() + " broadcasting membership in buddy pool " + buddyPoolName + " to recipients " + recipients);
      }

      MethodCall membershipCall = MethodCallFactory.create(MethodDeclarations.remoteAnnounceBuddyPoolNameMethod, new Object[]{buddyGroup.getDataOwner(), buddyPoolName});
      MethodCall replicateCall = MethodCallFactory.create(MethodDeclarations.replicateMethod, new Object[]{membershipCall});

      try
      {
         makeRemoteCall(recipients, replicateCall, true);
      }
      catch (Exception e)
      {
         log.error("Problems broadcasting buddy pool membership info to cluster", e);
      }
   }

   private void makeRemoteCall(List recipients, MethodCall call, boolean sync) throws Exception
   {
      // remove non-members from dest list
      if (recipients != null)
      {
         Iterator recipientsIt = recipients.iterator();
         List members = cache.getMembers();
         while (recipientsIt.hasNext())
         {
            if (!members.contains(recipientsIt.next()))
            {
               recipientsIt.remove();

            }
         }
      }

      cache.callRemoteMethods(recipients, call, sync, true, buddyCommunicationTimeout);
   }


   private void handleArgs(Object[] args, boolean transformForCurrentCall)
   {
      for (int i = 0; i < args.length; i++)
      {
         if (args[i] instanceof JBCMethodCall)
         {
            JBCMethodCall call = (JBCMethodCall) args[i];
            boolean transformFqns = true;
            if (call.getMethodId() == MethodDeclarations.dataGravitationCleanupMethod_id)
            {
               transformFqns = false;
            }

            args[i] = transformFqns((JBCMethodCall) args[i], transformFqns);
         }

         if (args[i] instanceof List && args[i] != null)
         {
            Object[] asArray = ((List) args[i]).toArray();
            handleArgs(asArray, transformForCurrentCall);
            List newList = new ArrayList(asArray.length);
            // Oops! JDK 5.0!
            //Collections.addAll(newList, asArray);
            newList.addAll(Arrays.asList(asArray));
            args[i] = newList;
         }

         if (args[i] instanceof Fqn)
         {
            Fqn fqn = (Fqn) args[i];
            if (transformForCurrentCall) args[i] = getBackupFqn(fqn);
         }
      }
   }

   /**
    * Assumes the backup Fqn if the current instance is the data owner
    *
    * @param originalFqn
    * @return backup fqn
    */
   public Fqn getBackupFqn(Fqn originalFqn)
   {
      return getBackupFqn(buddyGroup == null || buddyGroup.getGroupName() == null ? "null" : buddyGroup.getGroupName(), originalFqn);
   }

   /**
    * Blocks until the BuddyManager has finished initialising
    */
   private void waitForInit()
   {
      while (!initialised)
      {
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
         }
      }
   }

   public static Fqn getActualFqn(Fqn fqn)
   {
      if (!isBackupFqn(fqn)) return fqn;
      List elements = new ArrayList(fqn.peekElements());

      // remove the first 2 elements
      elements.remove(0);
      elements.remove(0);

      return new Fqn(elements);
   }


   /**
    * Asynchronous thread that deals with handling view changes placed on a queue
    */
   private class AsyncViewChangeHandlerThread implements Runnable
   {
      private Thread t;

      public void start()
      {
         if (t == null || !t.isAlive())
         {
            t = new Thread(this, "AsyncViewChangeHandlerThread-" + threadId.increment());
            t.setDaemon(true);
            t.start();
         }
      }

      public void run()
      {
         // don't start this thread until the Buddy Manager has initialised as it cocks things up.
         waitForInit();
         while (!Thread.interrupted())
         {
            try
            {
               handleEnqueuedViewChange();
            }
            catch (InterruptedException e)
            {
               break;
            }
            catch (Throwable t)
            {
               // Don't let the thread die
               log.error("Caught exception handling view change", t);
            }
         }
         log.trace("Exiting run()");
      }

      private void handleEnqueuedViewChange() throws Exception
      {
         log.trace("Waiting for enqueued view change events");
         List[] members = (List[]) queue.take(); // 2 element array - 0 - oldMembers, 1 - newMembers

         broadcastPoolMembership(members);

         boolean rebroadcast = false;

         // make sure new buddies have broadcast their pool memberships.
         while (!buddyPoolInfoAvailable(members[1]))
         {
            rebroadcast = true;
            synchronized (poolInfoNotifierLock)
            {
               log.trace("Not received necessary buddy pool info for all new members yet; waiting on poolInfoNotifierLock.");
               poolInfoNotifierLock.wait();
            }
         }

         if (rebroadcast) broadcastPoolMembership(members);

         // always refresh buddy list.
         reassignBuddies(members[1]);
      }

      private void broadcastPoolMembership(List[] members)
      {
         log.trace("Broadcasting pool membership details, triggered by view change.");
         if (members[0] == null)
            broadcastBuddyPoolMembership();
         else
         {
            List delta = new ArrayList();
            delta.addAll(members[1]);
            delta.removeAll(members[0]);
            broadcastBuddyPoolMembership(delta);
         }
      }

      private boolean buddyPoolInfoAvailable(List newMembers)
      {
         boolean infoReceived = true;
         Iterator i = newMembers.iterator();
         while (i.hasNext())
         {
            Object address = i.next();

            // make sure no one is concurrently writing to nullBuddyPool.
            synchronized(nullBuddyPool)
            {
               infoReceived = infoReceived && (address.equals(cache.getLocalAddress()) || buddyPool.keySet().contains(address) || nullBuddyPool.contains(address));
            }
         }

         if (log.isTraceEnabled()) log.trace(buddyGroup.getDataOwner() + " received buddy pool info for new members " + newMembers + "?  " + infoReceived);

         return infoReceived;
      }

   }
}

