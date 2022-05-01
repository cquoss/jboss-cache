/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import EDU.oswego.cs.dl.util.concurrent.CopyOnWriteArraySet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.buddyreplication.BuddyGroup;
import org.jboss.cache.buddyreplication.BuddyManager;
import org.jboss.cache.buddyreplication.BuddyNotInitException;
import org.jboss.cache.config.CacheLoaderConfig;
import org.jboss.cache.config.Option;
import org.jboss.cache.eviction.EvictionPolicy;
import org.jboss.cache.factories.InterceptorChainFactory;
import org.jboss.cache.factories.NodeFactory;
import org.jboss.cache.interceptors.Interceptor;
import org.jboss.cache.loader.CacheLoader;
import org.jboss.cache.loader.CacheLoaderManager;
import org.jboss.cache.loader.ExtendedCacheLoader;
import org.jboss.cache.loader.NodeData;
import org.jboss.cache.lock.IdentityLock;
import org.jboss.cache.lock.IsolationLevel;
import org.jboss.cache.lock.LockStrategyFactory;
import org.jboss.cache.lock.LockingException;
import org.jboss.cache.lock.TimeoutException;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.Region;
import org.jboss.cache.marshall.RegionManager;
import org.jboss.cache.marshall.RegionNameConflictException;
import org.jboss.cache.marshall.RegionNotFoundException;
import org.jboss.cache.marshall.TreeCacheMarshaller;
import org.jboss.cache.marshall.VersionAwareMarshaller;
import org.jboss.cache.optimistic.DataVersion;
import org.jboss.cache.statetransfer.StateTransferFactory;
import org.jboss.cache.statetransfer.StateTransferGenerator;
import org.jboss.cache.statetransfer.StateTransferIntegrator;
import org.jboss.cache.util.MBeanConfigurator;
import org.jboss.invocation.MarshalledValueOutputStream;
import org.jboss.system.ServiceMBeanSupport;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelClosedException;
import org.jgroups.ChannelNotConnectedException;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A tree-like structure that is replicated across several members. Updates are
 * multicast to all group members reliably and in order. User has the
 * option to set transaction isolation levels and other options.
 *
 * @author Bela Ban
 * @author Ben Wang
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Brian Stansberry
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Id: TreeCache.java 4983 2008-01-04 15:56:39Z manik.surtani@jboss.com $
 *          <p/>
 * @see <a href="http://labs.jboss.com/portal/jbosscache/docs">JBossCache doc</a>
 */
public class TreeCache extends ServiceMBeanSupport implements TreeCacheMBean, Cloneable, MembershipListener
{
   private static final String CREATE_MUX_CHANNEL = "createMultiplexerChannel";
   private static final String[] MUX_TYPES = {"java.lang.String", "java.lang.String"};
   private static final String JBOSS_SERVER_DOMAIN = "jboss";
   private static final String JGROUPS_JMX_DOMAIN = "jboss.jgroups";
   private static final String CHANNEL_JMX_ATTRIBUTES = "type=channel,cluster=";
   private static final String PROTOCOL_JMX_ATTRIBUTES = "type=protocol,cluster=";
   
   
   private boolean forceAnycast = false;
   /**
    * Default replication version, from {@link Version#getVersionShort}.
    */
   public static final short DEFAULT_REPLICATION_VERSION = Version.getVersionShort();

   // Quite poor, but for now, root may be re-initialised when setNodeLockingOptimistic() is called.
   // this is because if node locking is optimistic, we need to use OptimisticTreeNodes rather than TreeNodes.
   // - MANIK
   /**
    * Root DataNode.
    */
   protected DataNode root = NodeFactory.getInstance().createRootDataNode(NodeFactory.NODE_TYPE_TREENODE, this);

   /**
    * Set of TreeCacheListener.
    *
    * @see #addTreeCacheListener
    */
   private final Set listeners = new CopyOnWriteArraySet();

   // calling iterator on a ConcurrentHashMap is expensive due to synchronization - same problem
   // with calling isEmpty so hasListeners is an optimization to indicate whether or not listeners
   // is empty
   //
   /**
    * True if listeners are initialized.
    */
   protected boolean hasListeners = false;

   // store this seperately from other listeners to avoid concurrency penalty of
   // iterating through ConcurrentHashMap - eviction listener is always there (or almost always)
   // and there are less frequently other listeners so optimization is justified
   //
   TreeCacheListener evictionPolicyListener = null;

   final static Object NULL = new Object();

   /**
    * The JGroups JChannel in use.
    */
   protected JChannel channel = null;

   /**
    * The JGroups multiplexer service name; null if the multiplexer isn't used
    */
   protected String mux_serviceName = null;

   /**
    * The JGroups multiplexer stack name, default is "udp"
    */
   protected String mux_stackName = "udp";

   /**
    * Is this cache using a channel from the JGroups multiplexer
    */
   protected boolean using_mux = false;

   /**
    * True if this TreeCache is the coordinator.
    */
   protected boolean coordinator = false;

   /**
    * TreeCache log.
    */
   protected final static Log log = LogFactory.getLog(TreeCache.class);

   /**
    * Default cluster name.
    */
   protected String cluster_name = "TreeCache-Group";

   /**
    * Default cluster properties.
    *
    * @see #getClusterProperties
    */
   protected String cluster_props = null;

   /**
    * List of cluster group members.
    */
   protected final Vector members = new Vector();

   /**
    * JGroups RpcDispatcher in use.
    */
   protected RpcDispatcher disp = null;

   /**
    * JGroups message listener.
    */
   protected MessageListener ml = new MessageListenerAdaptor(log);

   /**
    * True if replication is queued.
    */
   protected boolean use_repl_queue = false;

   /**
    * Maximum number of replicated elements to queue.
    */
   protected int repl_queue_max_elements = 1000;

   /**
    * Replicated element queue interval.
    */
   protected long repl_queue_interval = 5000;

   /**
    * True if MBean interceptors are used.
    *
    * @see #getUseInterceptorMbeans
    */
   protected boolean use_interceptor_mbeans = true;

   /**
    * Maintains mapping of transactions (keys) and Modifications/Undo-Operations
    */
   private final TransactionTable tx_table = new TransactionTable();

   /**
    * HashMap<Thread, List<Lock>, maintains locks acquired by threads (used when no TXs are used)
    */
   private final Map lock_table = new ConcurrentHashMap();

   protected boolean fetchInMemoryState = true;

   protected boolean usingEviction = false;

   // These are private as the setters ensure consistency between them
   // - Brian
   private short replication_version = DEFAULT_REPLICATION_VERSION;
   private String repl_version_string = Version.getVersionString(DEFAULT_REPLICATION_VERSION);

   protected long lock_acquisition_timeout = 10000;
   protected long state_fetch_timeout = lock_acquisition_timeout + 5000;
   protected long sync_repl_timeout = 15000;
   protected String eviction_policy_class = null;
   protected int cache_mode = LOCAL;
   protected boolean inactiveOnStartup = false;
   protected boolean isStandalone = false;

   /**
    * Set<Fqn> of Fqns of the topmost node of internal regions that should
    * not included in standard state transfers.
    */
   protected Set internalFqns = new CopyOnWriteArraySet();

   /**
    * True if state was initialized during start-up.
    */
   protected boolean isStateSet = false;

   /**
    * An exception occuring upon fetch state.
    */
   protected Exception setStateException;
   private final Object stateLock = new Object();

   /**
    * Isolation level in use, default is {@link IsolationLevel#REPEATABLE_READ}.
    */
   protected IsolationLevel isolationLevel = IsolationLevel.REPEATABLE_READ;

   /**
    * Require write locks on parents before adding or removing children.  Default false.
    */
   protected boolean lockParentForChildInsertRemove = false;
   
   /**
    * This ThreadLocal contains an {@see InvocationContext} object, which holds
    * invocation-specific details.
    */
   private ThreadLocal invocationContextContainer = new ThreadLocal();

   /**
    * Eviction policy configuration in xml Element
    */
   protected Element evictConfig_ = null;


   protected String evictionInterceptorClass = "org.jboss.cache.interceptors.EvictionInterceptor";

   /**
    * True if we use region based marshalling.  Defaults to false.
    */
   protected boolean useRegionBasedMarshalling = false;

   /**
    * Marshaller if register to handle marshalling
    */
   protected VersionAwareMarshaller marshaller_ = null;

   /**
    * RegionManager used by marshaller and ExtendedCacheLoader
    */
   protected RegionManager regionManager_ = null;

   /**
    * RegionManager used by cache eviction
    */
   protected org.jboss.cache.eviction.RegionManager evictionRegionManager_ = null;

   /**
    * {@link #invokeMethod(MethodCall)} will dispatch to this chain of interceptors.
    * In the future, this will be replaced with JBossAop. This is a first step towards refactoring JBossCache.
    */
   protected Interceptor interceptor_chain = null;

   /**
    * Method to acquire a TransactionManager. By default we use JBossTransactionManagerLookup. Has
    * to be set before calling {@link #start()}
    */
   protected TransactionManagerLookup tm_lookup = null;

   /**
    * Class of the implementation of TransactionManagerLookup
    */
   protected String tm_lookup_class = null;

   /**
    * Used to get the Transaction associated with the current thread
    */
   protected TransactionManager tm = null;

   /**
    * The XML Element from which to configure the CacheLoader
    */
   protected Element cacheLoaderConfig = null;

   protected CacheLoaderManager cacheLoaderManager;
   /**
    * for legacy use *
    */
   protected CacheLoaderConfig cloaderConfig;

   /**
    * True if there is a synchronous commit phase, otherwise asynchronous commit.
    */
   protected boolean sync_commit_phase = false;

   /**
    * True if there is a synchronous rollback phase, otherwise asynchronous rollback.
    */
   protected boolean sync_rollback_phase = false;

   /**
    * @deprecated DO NOT USE THIS. IT IS HERE FOR EJB3 COMPILATION COMPATIBILITY WITH JBOSSCACHE1.3
    */
   protected EvictionPolicy eviction_policy_provider;

   /**
    * Queue used to replicate updates when mode is repl-async
    */
   protected ReplicationQueue repl_queue = null;

   /**
    * @deprecated use {@link Fqn#SEPARATOR}.
    */
   public static final String SEPARATOR = Fqn.SEPARATOR;

   /**
    * Entries in the cache are by default local and not replicated.
    */
   public static final int LOCAL = 1;

   /**
    * Entries in the cache are by default replicated asynchronously.
    */
   public static final int REPL_ASYNC = 2;

   /**
    * Entries in the cache are by default replicated synchronously.
    */
   public static final int REPL_SYNC = 3;

   /**
    * Cache sends {@link #evict} calls to remote caches when a node is changed.
    * {@link #evict} calls are asynchronous.
    */
   public static final int INVALIDATION_ASYNC = 4;

   /**
    * Cache sends {@link #evict} calls to remote caches when a node is changed.
    * {@link #evict} calls are synchronous.
    */
   public static final int INVALIDATION_SYNC = 5;

   /**
    * Uninitialized node key.
    */
   static public final String UNINITIALIZED = "jboss:internal:uninitialized";

   /**
    * Determines whether to use optimistic locking or not.  Disabled by default.
    */
   protected boolean nodeLockingOptimistic = false;

   /**
    * _createService was called.
    */
   protected boolean useCreateService = false;

   /**
    * Buddy replication configuration XML element
    */
   protected Element buddyReplicationConfig;

   /**
    * Buddy Manager
    */
   protected BuddyManager buddyManager;

   /**
    * Set of Fqns of nodes that are currently being processed by
    * activateReqion or inactivateRegion.  Requests for these fqns
    * will be ignored by _getState().
    */
   protected final Set activationChangeNodes = new HashSet();
   
   /**
    * The JGroups 2.4.1 or higher "callRemoteMethods" overload that 
    * provides Anycast support.
    */
   protected Method anycastMethod;
   
   /** Did we register our channel in JMX ourself? */
   protected boolean channelRegistered;   
   /** Did we register our channel's protocols in JMX ourself? */
   protected boolean protocolsRegistered;

   /**
    * Creates a channel with the given cluster name, properties, and state fetch timeout.
    */
   public TreeCache(String cluster_name, String props, long state_fetch_timeout) throws Exception
   {
      if (cluster_name != null)
         this.cluster_name = cluster_name;
      if (props != null)
         this.cluster_props = props;
      this.state_fetch_timeout = state_fetch_timeout;
   }

   /**
    * Constructs an uninitialized TreeCache.
    */
   public TreeCache() throws Exception
   {
   }

   /**
    * Constructs a TreeCache with an already connected channel.
    */
   public TreeCache(JChannel channel) throws Exception
   {
      this.channel = channel;
   }

   /**
    * Returns the TreeCache implementation version.
    */
   public String getVersion()
   {
      return Version.printVersion();
   }

   /**
    * Used internally by interceptors.
    * Don't use as client, this method will go away.
    */
   public DataNode getRoot()
   {
      return root;
   }

   /**
    * Returns the local channel address.
    */
   public Object getLocalAddress()
   {
      return channel != null ? channel.getLocalAddress() : null;
   }

   /**
    * Returns the members as a Vector.
    */
   public Vector getMembers()
   {
      return members;
   }

   /**
    * Returns <code>true</code> if this node is the group coordinator.
    */
   public boolean isCoordinator()
   {
      return coordinator;
   }

   /**
    * Returns the name of the replication cluster group.
    */
   public String getClusterName()
   {
      return cluster_name;
   }

   /**
    * Sets the name of the replication cluster group.
    */
   public void setClusterName(String name)
   {
      cluster_name = name;
   }

   /**
    * Returns the cluster properties as a String.
    * In the case of JGroups, returns the JGroup protocol stack specification.
    */
   public String getClusterProperties()
   {
      return cluster_props;
   }

   /**
    * Sets the cluster properties.
    * To use the new properties, the cache must be restarted using
    * {@link #stop} and {@link #start}.
    *
    * @param cluster_props The properties for the cluster (JGroups)
    */
   public void setClusterProperties(String cluster_props)
   {
      this.cluster_props = cluster_props;
   }

   public String getMultiplexerService()
   {
      return mux_serviceName;
   }

   public void setMultiplexerService(String serviceName)
   {
      mux_serviceName = serviceName;
   }

   public String getMultiplexerStack()
   {
      return mux_stackName;
   }

   public void setMultiplexerStack(String name)
   {
      mux_stackName = name;
   }

   /**
    * Gets whether this cache using a channel from the JGroups multiplexer.
    * Will not provide a meaningful result until after {@link #startService()}
    * is invoked.
    */
   public boolean isUsingMultiplexer()
   {
      return using_mux;
   }

   public boolean isForceAnycast()
   {
      return forceAnycast;
   }

   public void setForceAnycast(boolean b)
   {
      forceAnycast = b;
   }

   /**
    * Returns the transaction table.
    */
   public TransactionTable getTransactionTable()
   {
      return tx_table;
   }

   /**
    * Returns the lock table.
    */
   public Map getLockTable()
   {
      return lock_table;
   }

   /**
    * Returns the contents of the TransactionTable as a string.
    */
   public String dumpTransactionTable()
   {
      return tx_table.toString(true);
   }

   /**
    * Returns false.
    *
    * @deprecated
    */
   public boolean getDeadlockDetection()
   {
      return false;
   }

   /**
    * Does nothing.
    *
    * @deprecated
    */
   public void setDeadlockDetection(boolean dt)
   {
      log.warn("Using deprecated configuration element 'DeadlockDetection'.  Will be ignored.");
   }

   /**
    * Returns the interceptor chain as a debug string.
    * Returns &lt;empty&gt; if not initialized.
    */
   public String getInterceptorChain()
   {
      String retval = InterceptorChainFactory.printInterceptorChain(interceptor_chain);
      if (retval == null || retval.length() == 0)
         return "<empty>";
      else
         return retval;
   }

   /**
    * Used for testing only - sets the interceptor chain.
    */
   public void setInterceptorChain(Interceptor i)
   {
      interceptor_chain = i;
   }

   /**
    * Returns the list of interceptors.
    */
   public List getInterceptors()
   {
      return InterceptorChainFactory.asList(interceptor_chain);
   }

   /**
    * Returns the cache loader configuration element.
    */
   public Element getCacheLoaderConfiguration()
   {
      return cacheLoaderConfig;
   }

   /**
    * Sets the cache loader configuration element.
    */
   public void setCacheLoaderConfiguration(Element cache_loader_config)
   {
      if (cloaderConfig != null)
         log.warn("Specified CacheLoaderConfig XML block will be ignored, because deprecated setters are used!");
      this.cacheLoaderConfig = cache_loader_config;
   }

   /**
    * Returns the underlying cache loader in use.
    */
   public CacheLoader getCacheLoader()
   {
      if (cacheLoaderManager == null) return null;
      return cacheLoaderManager.getCacheLoader();
   }

   /**
    * Used for PojoCache. No-op here.
    *
    * @param config
    * @throws CacheException
    */
   public void setPojoCacheConfig(Element config) throws CacheException
   {
      log.warn("setPojoCacheConfig(): You have a PojoCache config that is not used in TreeCache.");
   }

   public Element getPojoCacheConfig()
   {
      return null;
   }

   /**
    * Returns the MessageListener in use.
    */
   public MessageListener getMessageListener()
   {
      return ml;
   }

   /**
    * Returns whether the entire tree is inactive upon startup, only responding
    * to replication messages after {@link #activateRegion(String)} is called
    * to activate one or more parts of the tree.
    * <p/>
    * This property is only relevant if {@link #getUseMarshalling()} is
    * <code>true</code>.
    */
   public boolean isInactiveOnStartup()
   {
      return inactiveOnStartup;
   }

   /**
    * Sets whether the entire tree is inactive upon startup, only responding
    * to replication messages after {@link #activateRegion(String)} is
    * called to activage one or more parts of the tree.
    * <p/>
    * This property is only relevant if {@link #getUseMarshalling()} is
    * <code>true</code>.
    */
   public void setInactiveOnStartup(boolean inactiveOnStartup)
   {
      this.inactiveOnStartup = inactiveOnStartup;
   }

   /**
    * Returns if sync commit phase is used.
    */
   public boolean getSyncCommitPhase()
   {
      return sync_commit_phase;
   }

   /**
    * Sets if sync commit phase is used.
    */
   public void setSyncCommitPhase(boolean sync_commit_phase)
   {
      this.sync_commit_phase = sync_commit_phase;
   }

   /**
    * Returns if sync rollback phase is used.
    */
   public boolean getSyncRollbackPhase()
   {
      return sync_rollback_phase;
   }

   /**
    * Sets if sync rollback phase is used.
    */
   public void setSyncRollbackPhase(boolean sync_rollback_phase)
   {
      this.sync_rollback_phase = sync_rollback_phase;
   }

   /**
    * Sets the eviction policy configuration element.
    */
   public void setEvictionPolicyConfig(Element config)
   {
      evictConfig_ = config;
      if (log.isDebugEnabled()) log.debug("setEvictionPolicyConfig(): " + config);
   }

   /**
    * Returns the eviction policy configuration element.
    */
   public Element getEvictionPolicyConfig()
   {
      return evictConfig_;
   }

   public String getEvictionInterceptorClass()
   {
      return this.evictionInterceptorClass;
   }

   public boolean isUsingEviction()
   {
      return this.usingEviction;
   }

   public void setIsUsingEviction(boolean usingEviction)
   {
      this.usingEviction = usingEviction;
   }

   /**
    * Converts a list of elements to a Java Groups property string.
    */
   public void setClusterConfig(Element config)
   {
      StringBuffer buffer = new StringBuffer();
      NodeList stack = config.getChildNodes();
      int length = stack.getLength();

      for (int s = 0; s < length; s++)
      {
         org.w3c.dom.Node node = stack.item(s);
         if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
            continue;

         Element tag = (Element) node;
         String protocol = tag.getTagName();
         buffer.append(protocol);
         NamedNodeMap attrs = tag.getAttributes();
         int attrLength = attrs.getLength();
         if (attrLength > 0)
            buffer.append('(');
         for (int a = 0; a < attrLength; a++)
         {
            Attr attr = (Attr) attrs.item(a);
            String name = attr.getName();
            String value = attr.getValue();
            buffer.append(name);
            buffer.append('=');
            buffer.append(value);
            if (a < attrLength - 1)
               buffer.append(';');
         }
         if (attrLength > 0)
            buffer.append(')');
         buffer.append(':');
      }
      // Remove the trailing ':'
      buffer.setLength(buffer.length() - 1);
      setClusterProperties(buffer.toString());
      if (log.isDebugEnabled()) log.debug("setting cluster properties from xml to: " + cluster_props);
   }


   /**
    * Returns the max time to wait until the initial state is retrieved.
    * This is used in a replicating cache: when a new cache joins the cluster,
    * it needs to acquire the (replicated) state of the other members to
    * initialize itself. If no state has been received within <tt>timeout</tt>
    * milliseconds, the map will be not be initialized.
    *
    * @return long milliseconds to wait for the state; 0 means to wait forever
    */
   public long getInitialStateRetrievalTimeout()
   {
      return state_fetch_timeout;
   }

   /**
    * Sets the initial state transfer timeout.
    * see #getInitialStateRetrievalTimeout()
    */
   public void setInitialStateRetrievalTimeout(long timeout)
   {
      state_fetch_timeout = timeout;
   }

   /**
    * Returns the current caching mode. String values returned are:
    * <ul>
    * <li>LOCAL
    * <li>REPL_ASYNC
    * <li>REPL_SYNC
    * <li>INVALIDATION_ASYNC
    * <li>INVALIDATION_SYNC
    * <ul>
    *
    * @return the caching mode
    */
   public String getCacheMode()
   {
      return mode2String(cache_mode);
   }

   /**
    * Returns the internal caching mode as an integer.
    */
   public int getCacheModeInternal()
   {
      return cache_mode;
   }

   private String mode2String(int mode)
   {
      switch (mode)
      {
         case LOCAL:
            return "LOCAL";
         case REPL_ASYNC:
            return "REPL_ASYNC";
         case REPL_SYNC:
            return "REPL_SYNC";
         case INVALIDATION_ASYNC:
            return "INVALIDATION_ASYNC";
         case INVALIDATION_SYNC:
            return "INVALIDATION_SYNC";
         default:
            throw new RuntimeException("setCacheMode(): caching mode " + mode + " is invalid");
      }
   }

   /**
    * Sets the node locking scheme as a string.
    * If the scheme is <code>OPTIMISTIC</code>, uses optimistic locking.
    */
   public void setNodeLockingScheme(String s)
   {
      if (s != null)
         setNodeLockingOptimistic(s.trim().equalsIgnoreCase("OPTIMISTIC"));
   }

   /**
    * Returns the node locking scheme as a string.
    * Either <code>OPTIMISTIC</code> or <code>PESSIMISTIC</code>.
    */
   public String getNodeLockingScheme()
   {
      return nodeLockingOptimistic ? "OPTIMISTIC" : "PESSIMISTIC";
   }

   /**
    * Sets whether to use optimistic locking on the nodes.
    */
   protected void setNodeLockingOptimistic(boolean b)
   {
      nodeLockingOptimistic = b;
      if (b)
      {
         root = NodeFactory.getInstance().createRootDataNode(NodeFactory.NODE_TYPE_OPTIMISTIC_NODE, this);
         // ignore any isolation levels set
         isolationLevel = null;
      }
   }

   /**
    * Returns true if the node locking is optimistic.
    */
   public boolean isNodeLockingOptimistic()
   {
      return nodeLockingOptimistic;
   }

   /**
    * Sets the default caching mode.
    * One of:
    * <ul>
    * <li> local
    * <li> repl-async
    * <li> repl-sync
    * <li> invalidation-async
    * <li> invalidation-sync
    * </ul>
    */
   public void setCacheMode(String mode) throws Exception
   {
      int m = string2Mode(mode);
      setCacheMode(m);
   }


   /**
    * Sets the default cache mode. Valid arguments are
    * <ol>
    * <li>TreeCache.LOCAL
    * <li>TreeCache.REPL_ASYNC
    * <li>TreeCache.REPL_SYNC
    * </ol>
    *
    * @param mode
    */
   public void setCacheMode(int mode)
   {
      if (mode == LOCAL || mode == REPL_ASYNC || mode == REPL_SYNC || mode == INVALIDATION_ASYNC || mode == INVALIDATION_SYNC)
         this.cache_mode = mode;
      else
         throw new IllegalArgumentException("setCacheMode(): caching mode " + mode + " is invalid");
   }


   /**
    * Returns the default max timeout after which synchronous replication calls return.
    *
    * @return long Number of milliseconds after which a sync repl call must return. 0 means to wait forever
    */
   public long getSyncReplTimeout()
   {
      return sync_repl_timeout;
   }

   /**
    * Sets the default maximum wait time for synchronous replication to receive all results
    */
   public void setSyncReplTimeout(long timeout)
   {
      sync_repl_timeout = timeout;
   }

   /**
    * Returns true if the replication queue is being used.
    */
   public boolean getUseReplQueue()
   {
      return use_repl_queue;
   }

   /**
    * Sets if the replication queue should be used.
    * If so, it is started.
    */
   public void setUseReplQueue(boolean flag)
   {
      use_repl_queue = flag;
      if (flag)
      {
         if (repl_queue == null)
         {
            repl_queue = new ReplicationQueue(this, repl_queue_interval, repl_queue_max_elements);
            if (repl_queue_interval >= 0)
               repl_queue.start();
         }
      }
      else
      {
         if (repl_queue != null)
         {
            repl_queue.stop();
            repl_queue = null;
         }
      }
   }

   /**
    * Returns the replication queue interval.
    */
   public long getReplQueueInterval()
   {
      return repl_queue_interval;
   }

   /**
    * Sets the replication queue interval.
    */
   public void setReplQueueInterval(long interval)
   {
      this.repl_queue_interval = interval;
      if (repl_queue != null)
         repl_queue.setInterval(interval);
   }

   /**
    * Returns the replication queue max elements size.
    */
   public int getReplQueueMaxElements()
   {
      return repl_queue_max_elements;
   }

   /**
    * Sets the replication queue max elements size.
    */
   public void setReplQueueMaxElements(int max_elements)
   {
      this.repl_queue_max_elements = max_elements;
      if (repl_queue != null)
         repl_queue.setMax_elements(max_elements);
   }

   /**
    * Returns the replication queue.
    */
   public ReplicationQueue getReplQueue()
   {
      return repl_queue;
   }

   /**
    * Returns the transaction isolation level.
    */
   public String getIsolationLevel()
   {
      return isolationLevel.toString();
   }

   /**
    * Set the transaction isolation level. This determines the locking strategy to be used
    */
   public void setIsolationLevel(String level)
   {
      IsolationLevel tmp_level = IsolationLevel.stringToIsolationLevel(level);

      if (tmp_level == null)
      {
         throw new IllegalArgumentException("TreeCache.setIsolationLevel(): level \"" + level + "\" is invalid");
      }
      setIsolationLevel(tmp_level);
   }

   /**
    * @param level
    */
   public void setIsolationLevel(IsolationLevel level)
   {
      isolationLevel = level;
      LockStrategyFactory.setIsolationLevel(level);
   }

   public IsolationLevel getIsolationLevelClass()
   {
      return isolationLevel;
   }  

   /**
    * Gets whether inserting or removing a node requires a write lock
    * on the node's parent (when pessimistic locking is used.)
    * <p/>
    * The default value is <code>false</code>
    */
   public boolean getLockParentForChildInsertRemove()
   {
      return lockParentForChildInsertRemove;
   }

   /**
    * Sets whether inserting or removing a node requires a write lock
    * on the node's parent (when pessimistic locking is used.)
    * <p/>
    * The default value is <code>false</code>
    */
   public void setLockParentForChildInsertRemove(boolean lockParentForChildInsertRemove)
   {
      this.lockParentForChildInsertRemove = lockParentForChildInsertRemove;
   }


   public boolean getFetchStateOnStartup()
   {
      return !inactiveOnStartup && buddyManager == null
              && (fetchInMemoryState || getFetchPersistentState());
   }


   public void setFetchStateOnStartup(boolean flag)
   {
      log.warn("Calls to setFetchStateOnStartup are ignored; configure state " +
              "transfer using setFetchInMemoryState and any cache loader's " +
              "FetchPersistentState property");
   }

   public void setFetchInMemoryState(boolean flag)
   {
      fetchInMemoryState = flag;
   }

   public boolean getFetchInMemoryState()
   {
      return fetchInMemoryState;
   }

   /**
    * Gets whether persistent state should be included in any state transfer.
    *
    * @return <code>true</code> if there is a cache loader that has its
    *         <code>FetchPersistentState</code> property set to <code>true</code>
    */
   public boolean getFetchPersistentState()
   {
      // Removed shared flag on the cache loader (as it originally was) since,
      // from a conversation with Brian S.:
      // "The <shared> and <fetchPersistentState> elements are largely redundant.  If shared is true, we ignore fetchPersistentState.
      // If shared is false, we only transfer persistent state if fetchPersistentState is true, but I can't think of a use cache where
      // you'd use a non-shared cache loader and not fetchPersistentState.  So maybe we can get rid of <fetchPersistentState> and just
      // use <shared>"
      // - Manik, 9 Nov 05

      return cacheLoaderManager != null && cacheLoaderManager.isFetchPersistentState();
   }

   /**
    * Default max time to wait for a lock. If the lock cannot be acquired within this time, a LockingException will be thrown.
    *
    * @return long Max number of milliseconds to wait for a lock to be acquired
    */
   public long getLockAcquisitionTimeout()
   {
      return lock_acquisition_timeout;
   }

   /**
    * Set the max time for lock acquisition. A value of 0 means to wait forever (not recomended).
    * Note that lock acquisition timeouts may be removed in the future when we have deadlock detection.
    *
    * @param timeout
    */
   public void setLockAcquisitionTimeout(long timeout)
   {
      this.lock_acquisition_timeout = timeout;
   }


   /**
    * Returns the name of the cache eviction policy (must be an implementation of EvictionPolicy)
    *
    * @return Fully qualified name of a class implementing the EvictionPolicy interface
    */
   public String getEvictionPolicyClass()
   {
      return eviction_policy_class;
   }

   /**
    * Sets the classname of the eviction policy
    */
   public void setEvictionPolicyClass(String eviction_policy_class)
   {
      /*
      if (eviction_policy_class == null || eviction_policy_class.length() == 0)
      {
         return;
      }
      */

      this.eviction_policy_class = eviction_policy_class;
   }

   /**
    * Obtain eviction thread (if any) wake up interval in seconds
    */
   public int getEvictionThreadWakeupIntervalSeconds()
   {
      return evictionRegionManager_.getEvictionThreadWakeupIntervalSeconds();
   }


   /**
    * Sets the TransactionManagerLookup object
    *
    * @param l
    */
   public void setTransactionManagerLookup(TransactionManagerLookup l)
   {
      this.tm_lookup = l;
   }


   /**
    */
   public String getTransactionManagerLookupClass()
   {
      return tm_lookup_class;
   }

   /**
    * Sets the class of the TransactionManagerLookup impl. This will attempt to create an
    * instance, and will throw an exception if this fails.
    *
    * @param cl
    * @throws Exception
    */
   public void setTransactionManagerLookupClass(String cl) throws Exception
   {
      this.tm_lookup_class = cl;
   }

   /**
    */
   public TransactionManager getTransactionManager()
   {
      return tm;
   }

   /**
    * Returns true if interceptor MBeans are in use.
    */
   public boolean getUseInterceptorMbeans()
   {
      return use_interceptor_mbeans;
   }

   /**
    * Sets if interceptor MBeans are in use.
    */
   public void setUseInterceptorMbeans(boolean useMbeans)
   {
      use_interceptor_mbeans = useMbeans;
   }

   /**
    * Returns <code>this</code>.
    */
   public TreeCache getInstance()
   {
      return this;
   }

   /**
    * Fetches the group state from the current coordinator. If successful, this
    * will trigger JChannel setState() call.
    */
   public void fetchState(long timeout) throws ChannelClosedException, ChannelNotConnectedException
   {
      if (channel == null)
         throw new ChannelNotConnectedException();
      boolean rc = channel.getState(null, timeout);
      if (log.isDebugEnabled())
      {
         if (rc)
            log.debug("fetchState(): state was retrieved successfully");
         else
            log.debug("fetchState(): state could not be retrieved (first member)");
      }
   }

   /**
    * Sets the eviction listener.
    */
   public void setEvictionListener(TreeCacheListener listener)
   {
      evictionPolicyListener = listener;
   }

   /**
    * Adds a tree cache listener.
    */
   public void addTreeCacheListener(TreeCacheListener listener)
   {
      // synchronize on listenrs just to
      // ensure hasListeners is set correctly
      // based on possibility of concurrent adds/removes
      //
      synchronized (listeners)
      {
         listeners.add(listener);
         hasListeners = true;
      }
   }

   /**
    * Removes a tree cache listener.
    */
   public void removeTreeCacheListener(TreeCacheListener listener)
   {
      // synchronize on listenrs just to
      // ensure hasListeners is set correctly
      // based on possibility of concurrent adds/removes
      //
      synchronized (listeners)
      {
         listeners.remove(listener);
         hasListeners = !listeners.isEmpty();
      }
   }

   /**
    * Returns a collection containing the listeners of this tree cache.
    */
   public Collection getTreeCacheListeners()
   {
      return Collections.unmodifiableCollection(listeners);
   }

   /* --------------------------- MBeanSupport ------------------------- */

   /**
    * Lifecycle method. This is like initialize. Same thing as calling <code>create</code>
    *
    * @throws Exception
    */
   public void createService() throws Exception
   {
      _createService();
   }

   protected void _createService() throws Exception
   {
      if (this.tm_lookup == null && this.tm_lookup_class != null)
      {
         Class clazz = Thread.currentThread().getContextClassLoader().loadClass(this.tm_lookup_class);
         this.tm_lookup = (TransactionManagerLookup) clazz.newInstance();
      }

      try
      {
         if (tm_lookup != null)
         {
            tm = tm_lookup.getTransactionManager();
         }
         else
         {
            if (nodeLockingOptimistic)
            {
               log.fatal("No transaction manager lookup class has been defined. Transactions cannot be used and thus OPTIMISTIC locking cannot be used");
            }
            else
            {
               log.info("No transaction manager lookup class has been defined. Transactions cannot be used");
            }
         }
      }
      catch (Exception e)
      {
         log.debug("failed looking up TransactionManager, will not use transactions", e);
      }

      // create cache loader
      if ((cacheLoaderConfig != null || cloaderConfig != null) && cacheLoaderManager == null)
      {
         initialiseCacheLoaderManager();
      }

      createEvictionPolicy();

      // build interceptor chain
      interceptor_chain = new InterceptorChainFactory().buildInterceptorChain(this);
      // register interceptor mbeans
      isStandalone = (this.getServiceName() == null);
      if (use_interceptor_mbeans)
      {
         MBeanServer mbserver = getMBeanServer();
         if (mbserver != null)
            MBeanConfigurator.registerInterceptors(mbserver, this, isStandalone);
      }

      switch (cache_mode)
      {
         case LOCAL:
            log.debug("cache mode is local, will not create the channel");
            break;
         case REPL_SYNC:
         case REPL_ASYNC:
         case INVALIDATION_ASYNC:
         case INVALIDATION_SYNC:
            log.debug("cache mode is " + mode2String(cache_mode));
            if (channel != null)
            { // already started
               log.info("channel is already running");
               return;
            }
            if (mux_serviceName != null)
            {
               channel = getMultiplexerChannel(mux_serviceName, mux_stackName);
            }
            if (channel != null)
            {
               // we have a mux channel
               using_mux = true;
               log.info("Created Multiplexer Channel for cache cluster " + cluster_name +
                       " using stack " + getMultiplexerStack());
            }
            else
            {
               if (cluster_props == null)
               {
                  cluster_props = getDefaultProperties();
                  log.debug("setting cluster properties to default value");
               }
               channel = new JChannel(cluster_props);
               // JBCACHE-1048 Hack to register the channel
               registerChannelInJmx();
               
               // Only set GET_STATE_EVENTS if the JGroups version is < 2.3
               int jgVer= org.jgroups.Version.version;
               while (jgVer >= 100)
                  jgVer = (jgVer / 10);
               if (jgVer < 23)
               {
                  channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
               }
            }
            channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
            channel.setOpt(Channel.AUTO_GETSTATE, Boolean.TRUE);

/* Used for JMX jconsole for JDK5.0
            ArrayList servers=MBeanServerFactory.findMBeanServer(null);
            if(servers == null || servers.size() == 0) {
                throw new Exception("No MBeanServers found;" +
                                    "\nJmxTest needs to be run with an MBeanServer present, or inside JDK 5"); }
            MBeanServer server=(MBeanServer)servers.get(0);
            JmxConfigurator.registerChannel(channel, server, "JGroups:channel=" + channel.getChannelName() , true);
*/
            disp = new RpcDispatcher(channel, ml, this, this);
            disp.setMarshaller(getMarshaller());
            
            // See if Anycast is supported
            try
            {
               anycastMethod = disp.getClass().getMethod("callRemoteMethods", new Class[]{Vector.class, MethodCall.class, int.class, long.class, boolean.class});
            }
            catch (Throwable ignored)
            {
               log.debug("JGroups release " + org.jgroups.Version.version + 
                         " does not support anycast; will not use it");
            }
            break;
         default:
            throw new IllegalArgumentException("cache mode " + cache_mode + " is invalid");
      }

      useCreateService = true;
   }

   /**
    * Lifecyle method. This is the same thing as calling <code>start</code>
    *
    * @throws Exception
    */
   public void startService() throws Exception
   {

      // Get around the problem of standalone user forgets to call createService.
      if (!useCreateService)
         _createService();

      // cache loaders should be initialised *before* any state transfers take place to prevent
      // exceptions involving cache loaders not being started. - Manik
      if (cacheLoaderManager != null)
      {
         cacheLoaderManager.startCacheLoader();
      }

      boolean startBuddyManager = false;

      switch (cache_mode)
      {
         case LOCAL:
            break;
         case REPL_SYNC:
         case REPL_ASYNC:
         case INVALIDATION_ASYNC:
         case INVALIDATION_SYNC:
            channel.connect(cluster_name);

            if (log.isInfoEnabled()) log.info("TreeCache local address is " + channel.getLocalAddress());
            if (getFetchStateOnStartup())
            {
               try
               {
                  fetchStateOnStartup();
               }
               catch (Exception e)
               {
                  // make sure we disconnect from the channel before we throw this exception!
                  // JBCACHE-761
                  channel.disconnect();
                  channel.close();
                  throw e;
               }
            }
            startBuddyManager = true;
            break;
         default:
            throw new IllegalArgumentException("cache mode " + cache_mode + " is invalid");
      }

      //now attempt to preload the cache from the loader - Manik
      if (cacheLoaderManager != null)
      {
         cacheLoaderManager.preloadCache();
      }

      // Find out if we are coordinator (blocks until view is received)
      // TODO should this be moved above the buddy manager code??
      determineCoordinator();

      if (buddyManager != null && startBuddyManager)
      {
         buddyManager.init(this);
         if (use_repl_queue)
         {
            log.warn("Replication queue not supported when using buddy replication.  Disabling repliction queue.");
            use_repl_queue = false;
            repl_queue = null;
         }
      }


      notifyCacheStarted();
   }

   /**
    * Unregisters the interceptor MBeans.
    */
   public void destroyService()
   {
      // unregister interceptor mbeans
      if (use_interceptor_mbeans)
      {
         MBeanServer mbserver = getMBeanServer();
         if (mbserver != null)
         {
            try
            {
               MBeanConfigurator.unregisterInterceptors(mbserver, this, isStandalone);
            }
            catch (Exception e)
            {
               log.error("failed unregistering cache interceptor mbeans ", e);
            }
         }
      }
   }


   /**
    * Lifecycle method. Same thing as calling <code>stop</code>.
    */
   public void stopService()
   {
      if (channel != null)
      {
         log.info("stopService(): closing the channel");
         channel.close();
         // JBCACHE-1048 Hack
         unregisterChannelFromJmx();
         channel = null;
      }
      if (disp != null)
      {
         log.info("stopService(): stopping the dispatcher");
         disp.stop();
         disp = null;
      }
      if (members != null && members.size() > 0)
         members.clear();

      coordinator = false;

      if (repl_queue != null)
         repl_queue.stop();

      if (cacheLoaderManager != null)
      {
         cacheLoaderManager.stopCacheLoader();
      }

      notifyCacheStopped();

      // Need to clean up listeners as well
      listeners.clear();
      hasListeners = false;

      evictionPolicyListener = null;

      useCreateService = false;
   }

   /* ----------------------- End of MBeanSupport ----------------------- */

   /* ----------------------- Start of buddy replication specific methods ------------*/

   /**
    * Sets the buddy replication configuration element
    *
    * @param config
    */
   public void setBuddyReplicationConfig(Element config)
   {
      if (config != null)
      {
         buddyReplicationConfig = config;
         buddyManager = new BuddyManager(config);
         if (!buddyManager.isEnabled())
            buddyManager = null;
         else
            internalFqns.add(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN);
      }
   }

   /**
    * Retrieves the buddy replication cofiguration element
    *
    * @return config
    */
   public Element getBuddyReplicationConfig()
   {
      return buddyReplicationConfig;
   }

   /**
    * Retrieves the Buddy Manager configured.
    *
    * @return null if buddy replication is not enabled.
    */
   public BuddyManager getBuddyManager()
   {
      return buddyManager;
   }

   /**
    * Returns a Set<Fqn> of Fqns of the topmost node of internal regions that
    * should not included in standard state transfers. Will include
    * {@link BuddyManager#BUDDY_BACKUP_SUBTREE} if buddy replication is
    * enabled.
    *
    * @return an unmodifiable Set<Fqn>.  Will not return <code>null</code>.
    */
   public Set getInternalFqns()
   {
      return Collections.unmodifiableSet(internalFqns);
   }

   /* ----------------------- End of buddy replication specific methods ------------*/

   protected void createEvictionPolicy()
   {
      // Configure if eviction policy is set (we test the old style 1.2.x style config and 1.3 style config.
      evictionRegionManager_ = new org.jboss.cache.eviction.RegionManager();
      if ((eviction_policy_class != null && eviction_policy_class.length() > 0) ||
              (org.jboss.cache.eviction.RegionManager.isUsingNewStyleConfiguration(this.getEvictionPolicyConfig())))
      {
         evictionRegionManager_.configure(this);
         this.usingEviction = true;
      }
      else
      {
         this.usingEviction = false;
         log.debug("Not using an EvictionPolicy");
      }
   }

   /**
    * @deprecated DO NOT USE THIS METHOD. IT IS PROVIDED FOR FULL JBCACHE 1.2 API BACKWARDS COMPATIBILITY
    */
   public void setEvictionPolicyProvider(EvictionPolicy policy)
   {
      log.debug("Using deprecated configuration element 'EvictionPolicyProvider'.  This is only provided for 1.2.x backward compatibility and may disappear in future releases.");
      this.eviction_policy_provider = policy;
   }

   /**
    * Sets if the TreeCache will use marshalling.
    * <p/>
    * Will ALWAYS use marshalling now.  This is now synonymous with setRegionBasedMarshalling
    *
    * @see #setUseRegionBasedMarshalling(boolean)
    * @deprecated
    */
   public void setUseMarshalling(boolean isTrue)
   {
      log.warn("Using deprecated configuration element 'UseMarshalling'.  See 'UseRegionBasedMarshalling' instead.");
      useRegionBasedMarshalling = isTrue;
   }

   /**
    * Returns true if the TreeCache will use marshalling.
    * <p/>
    * Will ALWAYS use marshalling now.  This is now synonymous with setRegionBasedMarshalling
    *
    * @see #getUseRegionBasedMarshalling()
    * @deprecated
    */
   public boolean getUseMarshalling()
   {
      return useRegionBasedMarshalling;
   }

   /**
    * Sets whether marshalling uses scoped class loaders on a per region basis.
    * <p/>
    * This property must be set to <code>true</code> before any call to
    * {@link #registerClassLoader(String,ClassLoader)} or
    * {@link #activateRegion(String)}
    *
    * @param isTrue
    */
   public void setUseRegionBasedMarshalling(boolean isTrue)
   {
      this.useRegionBasedMarshalling = isTrue;
   }

   /**
    * Tests whether region based marshaling s used.
    *
    * @return true if region based marshalling is used.
    */
   public boolean getUseRegionBasedMarshalling()
   {
      return useRegionBasedMarshalling;
   }

   /**
    * Loads the indicated Fqn, plus all parents recursively from the
    * CacheLoader. If no CacheLoader is present, this is a no-op
    *
    * @param fqn
    * @throws Exception
    */
   public void load(String fqn) throws Exception
   {
      if (cacheLoaderManager != null)
      {
         cacheLoaderManager.preload(Fqn.fromString(fqn), true, true);
      }
   }

   protected boolean determineCoordinator()
   {
      // Synchronize on members to make the answer atomic for the current view
      synchronized (members)
      {
         Address coord = getCoordinator();
         coordinator = (coord == null ? false : coord.equals(getLocalAddress()));
         return coordinator;
      }
   }

   /**
    * Returns the address of the coordinator or null if there is no
    * coordinator.
    */
   public Address getCoordinator()
   {
      if (channel == null)
         return null;

      synchronized (members)
      {
         if (members.size() == 0)
         {
            log.debug("getCoordinator(): waiting on viewAccepted()");
            try
            {
               members.wait();
            }
            catch (InterruptedException iex)
            {
               log.error("getCoordinator(): Interrupted while waiting for members to be set", iex);
            }
         }
         return members.size() > 0 ? (Address) members.get(0) : null;
      }
   }

   // -----------  Marshalling and State Transfer -----------------------

   /**
    * Returns the state bytes from the message listener.
    */
   public byte[] getStateBytes()
   {
      return this.getMessageListener().getState();
   }

   /**
    * Sets the state bytes in the message listener.
    */
   public void setStateBytes(byte[] state)
   {
      this.getMessageListener().setState(state);
   }

   /**
    * Registers a specific classloader for a region defined by a fully
    * qualified name.
    * A instance of {@link TreeCacheMarshaller} is used for marshalling.
    *
    * @param fqn The fqn region. Children of this fqn will use this classloader for (un)marshalling.
    * @param cl  The class loader to use
    * @throws RegionNameConflictException If there is a conflict in existing registering for the fqn.
    * @throws IllegalStateException       if marshalling is not being used
    * @see #getUseMarshalling
    * @see #getMarshaller
    */
   public void registerClassLoader(String fqn, ClassLoader cl)
           throws RegionNameConflictException
   {
      if (!useRegionBasedMarshalling)
         throw new IllegalStateException("useRegionBasedMarshalling is false; cannot use this method");

      // Use the getter method here, as it will create the marshaller
      // if this method is called before we do it in _createService()
      getMarshaller().registerClassLoader(fqn, cl);
   }

   /**
    * Unregisteres a class loader for a region.
    *
    * @param fqn The fqn region.
    * @throws RegionNotFoundException If there is a conflict in fqn specification.
    * @throws IllegalStateException   if marshalling is not being used
    * @see #getUseMarshalling
    */
   public void unregisterClassLoader(String fqn) throws RegionNotFoundException
   {
      if (!useRegionBasedMarshalling)
         throw new IllegalStateException("useRegionBasedMarshalling is false; cannot use this method");

      // Use the getter method here, as it will create the marshaller
      // if this method is called before we do it in _createService()
      getMarshaller().unregisterClassLoader(fqn);
   }

   /**
    * Causes the cache to transfer state for the subtree rooted at
    * <code>subtreeFqn</code> and to begin accepting replication messages
    * for that subtree.
    * <p/>
    * <strong>NOTE:</strong> This method will cause the creation of a node
    * in the local tree at <code>subtreeFqn</code> whether or not that
    * node exists anywhere else in the cluster.  If the node does not exist
    * elsewhere, the local node will be empty.  The creation of this node will
    * not be replicated.
    *
    * @param subtreeFqn Fqn string indicating the uppermost node in the
    *                   portion of the tree that should be activated.
    * @throws RegionNotEmptyException if the node <code>subtreeFqn</code>
    *                                 exists and has either data or children
    * @throws IllegalStateException   if {@link #getUseMarshalling() useMarshalling} is <code>false</code>
    */
   public void activateRegion(String subtreeFqn)
           throws RegionNotEmptyException, RegionNameConflictException, CacheException
   {
      if (!useRegionBasedMarshalling)
         throw new IllegalStateException("TreeCache.activateRegion(). useRegionBasedMarshalling flag is not set!");

      Fqn fqn = Fqn.fromString(subtreeFqn);

      // Check whether the node already exists and has data
      DataNode subtreeRoot = findNode(fqn);
      if (!(isNodeEmpty(subtreeRoot)))
      {
         throw new RegionNotEmptyException("Node " + subtreeRoot.getFqn() +
                 " already exists and is not empty");
      }

      if (log.isDebugEnabled())
         log.debug("activating " + fqn);

      try
      {

         // Add this fqn to the set of those we are activating
         // so calls to _getState for the fqn can return quickly
         synchronized (activationChangeNodes)
         {
            activationChangeNodes.add(fqn);
         }

         // Start accepting messages for the subtree, but
         // queue them for later processing.  We do this early
         // to reduce the chance of discarding a prepare call
         // whose corresponding commit will thus fail after activation
         Region region = regionManager_.getRegion(fqn);
         if (region == null)
            region = regionManager_.createRegion(fqn, null, true);

         region.startQueuing();

         // If a classloader is registered for the node's region, use it
         ClassLoader cl = region.getClassLoader();

         // Request partial state from the cluster and integrate it
         if (BuddyManager.isBackupFqn(fqn))
         {
            if (log.isDebugEnabled())
               log.debug("Not attempting to load state for a buddy backup Fqn that has just been activated: " + fqn);
         }
         else
         {
            if (buddyManager == null)
            {
               // Get the state from any node that has it and put it
               // in the main tree
               if (subtreeRoot == null)
               {
                  // We'll update this node with the state we receive
                  subtreeRoot = createSubtreeRootNode(fqn);
               }
               loadState(subtreeRoot, cl);
            }
            else
            {
               // Get the state from each DataOwner and integrate in their
               // respective buddy backup tree
               List buddies = buddyManager.getBackupDataOwners(); 
               for (Iterator it = buddies.iterator(); it.hasNext();)
               {
                  Address buddy = (Address) it.next();
                  if (getMembers() == null || !getMembers().contains(buddy))
                     continue;
                  
                  Object[] sources = {buddy};
                  Fqn base = new Fqn(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN, BuddyManager.getGroupNameFromAddress(buddy));
                  Fqn buddyRoot = new Fqn(base, fqn);
                  subtreeRoot = findNode(buddyRoot);
                  if (subtreeRoot == null)
                  {
                     // We'll update this node with the state we receive
                     subtreeRoot = createSubtreeRootNode(buddyRoot);
                  }
                  _loadState(fqn, subtreeRoot, sources, cl);
               }
            }
         }

         // Lock out other activity on the region while we
         // we process the queue and activate the region
         List queue = region.getMethodCallQueue();
         synchronized (queue)
         {
            processQueuedMethodCalls(queue);
            region.activate();
         }

      }
      catch (Throwable t)
      {
         log.error("failed to activate " + subtreeFqn, t);

         // "Re-inactivate" the region
         try
         {
            inactivateRegion(subtreeFqn);
         }
         catch (Exception e)
         {
            log.error("failed inactivating " + subtreeFqn, e);
            // just swallow this one and throw the first one
         }

         // Throw the exception on, wrapping if necessary
         if (t instanceof RegionNameConflictException)
            throw (RegionNameConflictException) t;
         else if (t instanceof RegionNotEmptyException)
            throw (RegionNotEmptyException) t;
         else if (t instanceof CacheException)
            throw (CacheException) t;
         else
            throw new CacheException(t.getClass().getName() + " " +
                    t.getLocalizedMessage(), t);
      }
      finally
      {
         synchronized (activationChangeNodes)
         {
            activationChangeNodes.remove(fqn);
         }
      }
   }

   /**
    * Returns whether the given node is empty; i.e. has no key/value pairs
    * in its data map and has no children.
    *
    * @param node the node. Can be <code>null</code>.
    * @return <code>true</code> if <code>node</code> is <code>null</code> or
    *         empty; <code>false</code> otherwise.
    */
   private boolean isNodeEmpty(DataNode node)
   {
      boolean empty = true;
      if (node != null)
      {
         if (node.hasChildren())
            empty = false;
         else
         {
            Set keys = node.getDataKeys();
            empty = (keys == null || keys.size() == 0);
         }
      }
      return empty;
   }

   /**
    * Requests state from each node in the cluster until it gets it or no
    * node replies with a timeout exception.  If state is returned,
    * integrates it into the given DataNode.  If no state is returned but a
    * node replies with a timeout exception, the calls will be repeated with
    * a longer timeout, until 3 attempts have been made.
    *
    * @param subtreeRoot the DataNode into which state should be integrated
    * @param cl          the classloader to use to unmarshal the state.
    *                    Can be <code>null</code>.
    * @throws Exception
    */
   private void loadState(DataNode subtreeRoot, ClassLoader cl)
           throws Exception
   {
      Object[] mbrArray = getMembers().toArray();
      _loadState(subtreeRoot.getFqn(), subtreeRoot, mbrArray, cl);
   }

   /**
    * Requests state from each of the given source nodes in the cluster
    * until it gets it or no node replies with a timeout exception.  If state
    * is returned, integrates it into the given DataNode.  If no state is
    * returned but a node replies with a timeout exception, the calls will be
    * repeated with a longer timeout, until 3 attempts have been made.
    *
    * @param subtreeRoot     Fqn of the topmost node in the subtree whose
    *                        state should be transferred.
    * @param integrationRoot Fqn of the node into which state should be integrated
    * @param sources         the cluster nodes to query for state
    * @param cl              the classloader to use to unmarshal the state.
    *                        Can be <code>null</code>.
    * @throws Exception
    */
   public void _loadState(Fqn subtreeRoot, Fqn integrationRoot,
                          Object[] sources, ClassLoader cl)
           throws Exception
   {
      DataNode target = findNode(integrationRoot);
      if (target == null)
      {
         // Create the integration root, but do not replicate
         Option option = new Option();
         option.setCacheModeLocal(true);
         this.put(integrationRoot, null, option);
         target = findNode(integrationRoot);
      }

      _loadState(subtreeRoot, target, sources, cl);
   }

   /**
    * Requests state from each of the given source nodes in the cluster
    * until it gets it or no node replies with a timeout exception.  If state
    * is returned, integrates it into the given DataNode.  If no state is
    * returned but a node replies with a timeout exception, the calls will be
    * repeated with a longer timeout, until 3 attempts have been made.
    *
    * @param subtreeRoot     Fqn of the topmost node in the subtree whose
    *                        state should be transferred.
    * @param integrationRoot the DataNode into which state should be integrated
    * @param sources         the cluster nodes to query for state
    * @param cl              the classloader to use to unmarshal the state.
    *                        Can be <code>null</code>.
    * @throws Exception
    */
   public void _loadState(Fqn subtreeRoot, DataNode integrationRoot,
                          Object[] sources, ClassLoader cl)
           throws Exception
   {
      // Call each node in the cluster with progressively longer timeouts
      // until we get state or no cluster node returns a TimeoutException
      long[] timeouts = {400, 800, 1600};
      Object ourself = getLocalAddress(); // ignore ourself when we call
      boolean stateSet = false;
      TimeoutException timeoutException = null;
      Object timeoutTarget = null;

      boolean trace = log.isTraceEnabled();

      for (int i = 0; i < timeouts.length; i++)
      {
         timeoutException = null;

         Boolean force = (i == timeouts.length - 1) ? Boolean.TRUE
                 : Boolean.FALSE;

         MethodCall psmc = MethodCallFactory.create(MethodDeclarations.getPartialStateMethod,
                 new Object[]{subtreeRoot,
                         new Long(timeouts[i]),
                         force,
                         Boolean.FALSE});

         MethodCall replPsmc = MethodCallFactory.create(MethodDeclarations.replicateMethod,
                 new Object[]{psmc});

         // Iterate over the group members, seeing if anyone
         // can give us state for this region
         for (int j = 0; j < sources.length; j++)
         {
            Object target = sources[j];
            if (ourself.equals(target))
               continue;

            Vector targets = new Vector();
            targets.add(target);

            List responses = null;
            try
            {
               responses = callRemoteMethods(targets, replPsmc, true,
                       true, sync_repl_timeout);
            }
            catch (Exception t)
            {
               if (!(t.getCause() instanceof TimeoutException)) throw t;
               timeoutException = (TimeoutException) t.getCause();
               timeoutTarget = target;
               if (trace)
               {
                  log.trace("TreeCache.activateRegion(): " + ourself +
                          " got a TimeoutException from " + target);
               }
               continue;
            }

            Object rsp = null;
            if (responses != null && responses.size() > 0)
            {
               rsp = responses.get(0);
               if (rsp instanceof byte[])
               {
                  _setState((byte[]) rsp, integrationRoot, cl);
                  stateSet = true;

                  if (log.isDebugEnabled())
                  {
                     log.debug("TreeCache.activateRegion(): " + ourself +
                             " got state from " + target);
                  }

                  break;
               }
               else if (rsp instanceof TimeoutException)
               {
                  timeoutException = (TimeoutException) rsp;
                  timeoutTarget = target;
                  if (trace)
                  {
                     log.trace("TreeCache.activateRegion(): " + ourself +
                             " got a TimeoutException from " + target);
                  }
               }
            }

            if (trace)
            {
               log.trace("TreeCache.activateRegion(): " + ourself +
                       " No usable response from node " + target +
                       (rsp == null ? "" : (" -- received " + rsp)));
            }
         }

         // We've looped through all targets; if we got state or didn't
         // but no one sent a timeout (which means no one had state)
         // we don't want to try again
         if (stateSet || timeoutException == null)
            break;
      }

      if (!stateSet)
      {
         // If we got a timeout exception on the final try,
         // this is a failure condition
         if (timeoutException != null)
         {
            throw new CacheException("Failed getting state due to timeout on " +
                    timeoutTarget, timeoutException);
         }

         if (log.isDebugEnabled())
            log.debug("TreeCache.activateRegion(): No nodes able to give state");
      }
   }

   /**
    * Creates a subtree in the local tree.
    * Returns the DataNode created.
    */
   protected DataNode createSubtreeRootNode(Fqn subtree) throws CacheException
   {
      DataNode parent = root;
      DataNode child = null;
      Object owner = getOwnerForLock();
      Object name = null;
      NodeFactory factory = NodeFactory.getInstance();
      byte type = isNodeLockingOptimistic()
              ? NodeFactory.NODE_TYPE_OPTIMISTIC_NODE
              : NodeFactory.NODE_TYPE_TREENODE;

      for (int i = 0; i < subtree.size(); i++)
      {
         name = subtree.get(i);
         child = (DataNode) parent.getChild(name);
         if (child == null)
         {
            // Lock the parent, create and add the child
            try
            {
               parent.acquire(owner, state_fetch_timeout, DataNode.LOCK_TYPE_WRITE);
            }
            catch (InterruptedException e)
            {
               log.error("Interrupted while locking" + parent.getFqn(), e);
               throw new CacheException(e.getLocalizedMessage(), e);
            }

            try
            {
               child = factory.createDataNode(type, name,
                       subtree.getFqnChild(i + 1),
                       parent, null, this);
               parent.addChild(name, child);
            }
            finally
            {
               if (log.isDebugEnabled())
                  log.debug("forcing release of locks in " + parent.getFqn());
               try
               {
                  parent.releaseForce();
               }
               catch (Throwable t)
               {
                  log.error("failed releasing locks", t);
               }
            }
         }

         parent = child;
      }

      return child;
   }

   /**
    * Causes the cache to stop accepting replication events for the subtree
    * rooted at <code>subtreeFqn</code> and evict all nodes in that subtree.
    *
    * @param subtreeFqn Fqn string indicating the uppermost node in the
    *                   portion of the tree that should be activated.
    * @throws RegionNameConflictException if <code>subtreeFqn</code> indicates
    *                                     a node that is part of another
    *                                     subtree that is being specially
    *                                     managed (either by activate/inactiveRegion()
    *                                     or by registerClassLoader())
    * @throws CacheException              if there is a problem evicting nodes
    * @throws IllegalStateException       if {@link #getUseMarshalling() useMarshalling} is <code>false</code>
    */
   public void inactivateRegion(String subtreeFqn) throws RegionNameConflictException, CacheException
   {
      if (!useRegionBasedMarshalling)
         throw new IllegalStateException("TreeCache.inactivate(). useRegionBasedMarshalling flag is not set!");

      Fqn fqn = Fqn.fromString(subtreeFqn);
      DataNode parent = null;
      DataNode subtreeRoot = null;
      boolean parentLocked = false;
      boolean subtreeLocked = false;

      if (log.isDebugEnabled())
         log.debug("inactivating " + fqn);

      try
      {

         synchronized (activationChangeNodes)
         {
            activationChangeNodes.add(fqn);
         }

         boolean inactive = marshaller_.isInactive(subtreeFqn);
         if (!inactive)
            marshaller_.inactivate(subtreeFqn);

         // Create a list with the Fqn in the main tree and any buddy backup trees
         ArrayList list = new ArrayList();
         list.add(fqn);
         if (buddyManager != null)
         {
            Set buddies = getChildrenNames(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN);
            if (buddies != null)
            {
               for (Iterator it = buddies.iterator(); it.hasNext();)
               {
                  Fqn base = new Fqn(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN, it.next());
                  list.add(new Fqn(base, fqn));
               }
            }
         }

         // Remove the subtree from the main tree  and any buddy backup trees
         for (Iterator it = list.iterator(); it.hasNext();)
         {
            Fqn subtree = (Fqn) it.next();
            subtreeRoot = findNode(subtree);
            if (subtreeRoot != null)
            {
               // Acquire locks
               Object owner = getOwnerForLock();
               subtreeRoot.acquireAll(owner, state_fetch_timeout, DataNode.LOCK_TYPE_WRITE);
               subtreeLocked = true;

               // Lock the parent, as we're about to write to it
               parent = (DataNode) subtreeRoot.getParent();
               if (parent != null)
               {
                  parent.acquire(owner, state_fetch_timeout, DataNode.LOCK_TYPE_WRITE);
                  parentLocked = true;
               }

               // Remove the subtree
               _evictSubtree(subtree);

               // Release locks
               if (parent != null)
               {
                  log.debug("forcing release of locks in parent");
                  parent.releaseAllForce();
               }

               parentLocked = false;

               log.debug("forcing release of all locks in subtree");
               subtreeRoot.releaseAllForce();
               subtreeLocked = false;
            }
         }
      }
      catch (InterruptedException ie)
      {
         throw new CacheException("Interrupted while acquiring lock", ie);
      }
      finally
      {
         // If we didn't succeed, undo the marshalling change
         // NO. Since we inactivated, we may have missed changes
         //if (!success && !inactive)
         //   marshaller_.activate(subtreeFqn);

         // If necessary, release locks
         if (parentLocked)
         {
            log.debug("forcing release of locks in parent");
            try
            {
               parent.releaseAllForce();
            }
            catch (Throwable t)
            {
               log.error("failed releasing locks", t);
            }
         }
         if (subtreeLocked)
         {
            log.debug("forcing release of all locks in subtree");
            try
            {
               subtreeRoot.releaseAllForce();
            }
            catch (Throwable t)
            {
               log.error("failed releasing locks", t);
            }
         }

         synchronized (activationChangeNodes)
         {
            activationChangeNodes.remove(fqn);
         }
      }
   }

   /**
    * Evicts the node at <code>subtree</code> along with all descendant nodes.
    *
    * @param subtree Fqn indicating the uppermost node in the
    *                portion of the tree that should be evicted.
    * @throws CacheException
    */
   protected void _evictSubtree(Fqn subtree) throws CacheException
   {

      if (!exists(subtree))
         return;   // node does not exist. Maybe it has been recursively removed.

      if (log.isTraceEnabled())
         log.trace("_evictSubtree(" + subtree + ")");

      // Recursively remove any children
      Set children = getChildrenNames(subtree);
      if (children != null)
      {
         Object[] kids = children.toArray();

         for (int i = 0; i < kids.length; i++)
         {
            Object s = kids[i];
            Fqn tmp = new Fqn(subtree, s);
            _remove(null,    // no tx
                    tmp,
                    false,   // no undo ops
                    false,   // no nodeEvent
                    true);   // is an eviction
         }
      }

      // Remove the root node of the subtree (unless it is root!)
      if (!subtree.isRoot()) _remove(null, subtree, false, false, true);

   }

   /**
    * Called internally to enqueue a method call.
    *
    * @param subtree FQN of the subtree region
    * @see Region#getMethodCallQueue
    */
   public void _enqueueMethodCall(String subtree, MethodCall call)
           throws Throwable
   {
      JBCMethodCall jbcCall = (JBCMethodCall) call;
      Fqn fqn = Fqn.fromString(subtree);
      Region region = regionManager_.getRegion(fqn);
      // JBCACHE-1225 -- handle buddy region fqns
      if (region == null && BuddyManager.isBackupFqn(fqn))
      {
         // Strip out the buddy group portion
         fqn = fqn.getFqnChild(2, fqn.size());
         region = regionManager_.getRegion(fqn);
      }
      if (region == null)
         throw new IllegalStateException("No region found for " + subtree);

      List queue = region.getMethodCallQueue();
      synchronized (queue)
      {
         // Confirm we're not active yet; if we are just invoke the method
         switch (region.getStatus())
         {
            case(Region.STATUS_ACTIVE):
               if (log.isTraceEnabled())
                  log.trace("_enqueueMethodCall(): Invoking " + call.getName() +
                          " on subtree " + subtree);
               call.invoke(this);
               break;

            case(Region.STATUS_QUEUEING):

               // Don't bother queueing a getState call

               if (jbcCall.getMethodId() == MethodDeclarations.replicateMethod_id)
               {
                  JBCMethodCall mc = (JBCMethodCall) call.getArgs()[0];
                  if (mc.getMethodId() == MethodDeclarations.getPartialStateMethod_id)
                     return;
               }
               if (log.isTraceEnabled())
                  log.trace("_enqueueMethodCall(): Enqueuing " + call.getName() +
                          " " + call.getArgs() + " on subtree " + subtree);
               queue.add(jbcCall);
               break;

            default:
               log.trace("_enqueueMethodCall(): Discarding " + call.getName() +
                       " on subtree " + subtree);
         }
      }
   }

   private void processQueuedMethodCalls(List queue) throws Throwable
   {
      Map gtxMap = new HashMap();
      JBCMethodCall call = null;
      JBCMethodCall wrapped = null;
      for (Iterator iter = queue.iterator(); iter.hasNext();)
      {
         call = (JBCMethodCall) iter.next();
         boolean forgive = false;
         if (call.getMethodId() == MethodDeclarations.replicateMethod_id)
         {
            Object[] args = call.getArgs();
            wrapped = (JBCMethodCall) args[0];
            switch (wrapped.getMethodId())
            {
               case MethodDeclarations.prepareMethod_id:
                  args = wrapped.getArgs();
                  gtxMap.put(args[0], NULL);
                  break;
               case MethodDeclarations.commitMethod_id:
               case MethodDeclarations.rollbackMethod_id:
                  args = wrapped.getArgs();
                  // If we didn't see the prepare earlier, we'll forgive
                  // any error when we invoke the commit/rollback
                  // TODO maybe just skip the commit/rollback?
//                  forgive = (gtxMap.remove(args[0]) == null);
                  if (gtxMap.remove(args[0]) == null)
                     continue;
                  break;
            }
         }

         if (log.isTraceEnabled())
            log.trace("processing queued method call " + call.getName());

         try
         {
            call.invoke(this);
         }
         catch (Exception e) // TODO maybe just catch ISE thrown by ReplInterceptor
         {

            if (!forgive)
               throw e;
         }
         finally
         {
            // Clear any invocation context from this thread
            setInvocationContext(null);
         }
      }
   }

   /**
    * Returns the state for the portion of the tree named by <code>fqn</code>.
    * <p/>
    * State returned is a serialized byte[][], element 0 is the transient state
    * (or null), and element 1 is the persistent state (or null).
    *
    * @param fqn            Fqn indicating the uppermost node in the
    *                       portion of the tree whose state should be returned.
    * @param timeout        max number of ms this method should wait to acquire
    *                       a read lock on the nodes being transferred
    * @param force          if a read lock cannot be acquired after
    *                       <code>timeout</code> ms, should the lock acquisition
    *                       be forced, and any existing transactions holding locks
    *                       on the nodes be rolled back? <strong>NOTE:</strong>
    *                       In release 1.2.4, this parameter has no effect.
    * @param suppressErrors should any Throwable thrown be suppressed?
    * @return a serialized byte[][], element 0 is the transient state
    *         (or null), and element 1 is the persistent state (or null).
    * @throws UnsupportedOperationException if persistent state transfer is
    *                                       enabled, the requested Fqn is not the root node, and the
    *                                       cache loader does not implement {@link ExtendedCacheLoader}.
    */
   public byte[] _getState(Fqn fqn, long timeout, boolean force, boolean suppressErrors) throws Throwable
   {
      return _getState(fqn, this.fetchInMemoryState, getFetchPersistentState(), timeout, force, suppressErrors);
   }

   /**
    * Returns the state for the portion of the tree named by <code>fqn</code>.
    * <p/>
    * State returned is a serialized byte[][], element 0 is the transient state
    * (or null), and element 1 is the persistent state (or null).
    *
    * @param fqn            Fqn indicating the uppermost node in the
    *                       portion of the tree whose state should be returned.
    * @param timeout        max number of ms this method should wait to acquire
    *                       a read lock on the nodes being transferred
    * @param force          if a read lock cannot be acquired after
    *                       <code>timeout</code> ms, should the lock acquisition
    *                       be forced, and any existing transactions holding locks
    *                       on the nodes be rolled back? <strong>NOTE:</strong>
    *                       In release 1.2.4, this parameter has no effect.
    * @param suppressErrors should any Throwable thrown be suppressed?
    * @return a serialized byte[][], element 0 is the transient state
    *         (or null), and element 1 is the persistent state (or null).
    * @throws UnsupportedOperationException if persistent state transfer is
    *                                       enabled, the requested Fqn is not the root node, and the
    *                                       cache loader does not implement {@link ExtendedCacheLoader}.
    */
   public byte[] _getState(Fqn fqn, boolean fetchTransientState, boolean fetchPersistentState, long timeout, boolean force, boolean suppressErrors) throws Throwable
   {

      if (marshaller_ != null)
      {
         // can't give state for regions currently being activated/inactivated
         synchronized (activationChangeNodes)
         {
            if (activationChangeNodes.contains(fqn))
            {
               if (log.isDebugEnabled())
                  log.debug("ignoring _getState() for " + fqn + " as it is being activated/inactivated");
               return null;
            }
         }

         // Can't give state for inactive nodes
         if (marshaller_.isInactive(fqn.toString()))
         {
            if (log.isDebugEnabled())
               log.debug("ignoring _getState() for inactive region " + fqn);
            return null;
         }
      }

      DataNode rootNode = findNode(fqn);
      if (rootNode == null)
         return null;

      boolean getRoot = rootNode.equals(root);

      if (fetchPersistentState && (!getRoot) &&
              !((cacheLoaderManager.isExtendedCacheLoader())))
      {
         throw new UnsupportedOperationException("Cache loader does not support " +
                 "ExtendedCacheLoader; partial state transfer not supported");
      }

      Object owner = getOwnerForLock();

      try
      {
         if (fetchTransientState || fetchPersistentState)
         {
            if (log.isDebugEnabled())
               log.info("locking the subtree at " + fqn + " to transfer state");
            acquireLocksForStateTransfer(rootNode, owner, timeout, true, force);
         }

         StateTransferGenerator generator =
                 StateTransferFactory.getStateTransferGenerator(this);

         return generator.generateStateTransfer(rootNode,
                 fetchTransientState,
                 fetchPersistentState,
                 suppressErrors);
      }
      finally
      {
         releaseStateTransferLocks(rootNode, owner, true);
      }
   }

   /**
    * Returns any state stored in the cache that needs to be propagated
    * along with the normal transient state in a subtree when
    * {@link #_getState(Fqn,long,boolean,boolean)} is called for an Fqn.  Typically this would be state
    * stored outside of the subtree that is somehow associated with the subtree.
    * <p/>
    * This method is designed for overriding by
    * {@link org.jboss.cache.aop.PojoCache}.
    * The implementation in this class returns <code>null</code>.
    * </p>
    * <p/>
    * This method will only be invoked if
    * {@link #getCacheLoaderFetchTransientState()} returns <code>true</code>.
    * </p>
    *
    * @param fqn     the fqn that represents the root node of the subtree.
    * @param timeout max number of ms this method should wait to acquire
    *                a read lock on the nodes being transferred
    * @param force   if a read lock cannot be acquired after
    *                <code>timeout</code> ms, should the lock acquisition
    *                be forced, and any existing transactions holding locks
    *                on the nodes be rolled back? <strong>NOTE:</strong>
    *                In release 1.2.4, this parameter has no effect.
    * @return a byte[] representing the marshalled form of any "associated" state,
    *         or <code>null</code>.  This implementation returns <code>null</code>.
    */
   protected byte[] _getAssociatedState(Fqn fqn, long timeout, boolean force) throws Exception
   {
      // default implementation does nothing
      return null;
   }

   /**
    * Set the portion of the cache rooted in <code>targetRoot</code>
    * to match the given state. Updates the contents of <code>targetRoot</code>
    * to reflect those in <code>new_state</code>.
    * <p/>
    * <strong>NOTE:</strong> This method performs no locking of nodes; it
    * is up to the caller to lock <code>targetRoot</code> before calling
    * this method.
    *
    * @param new_state  a serialized byte[][] array where element 0 is the
    *                   transient state (or null) , and element 1 is the
    *                   persistent state (or null)
    * @param targetRoot fqn of the node into which the state should be integrated
    * @param cl         classloader to use to unmarshal the state, or
    *                   <code>null</code> if the TCCL should be used
    */
   public void _setState(byte[] new_state, Fqn targetRoot, ClassLoader cl)
           throws Exception
   {
      DataNode target = findNode(targetRoot);
      if (target == null)
      {
         // Create the integration root, but do not replicate
         Option option = new Option();
         option.setCacheModeLocal(true);
         this.put(targetRoot, null, option);
         target = findNode(targetRoot);
      }

      _setState(new_state, target, cl);
   }

   /**
    * Set the portion of the cache rooted in <code>targetRoot</code>
    * to match the given state. Updates the contents of <code>targetRoot</code>
    * to reflect those in <code>new_state</code>.
    * <p/>
    * <strong>NOTE:</strong> This method performs no locking of nodes; it
    * is up to the caller to lock <code>targetRoot</code> before calling
    * this method.
    *
    * @param new_state  a serialized byte[][] array where element 0 is the
    *                   transient state (or null) , and element 1 is the
    *                   persistent state (or null)
    * @param targetRoot node into which the state should be integrated
    * @param cl         classloader to use to unmarshal the state, or
    *                   <code>null</code> if the TCCL should be used
    */
   private void _setState(byte[] new_state, DataNode targetRoot, ClassLoader cl)
           throws Exception
   {
      if (new_state == null)
      {
         log.info("new_state is null (may be first member in cluster)");
         return;
      }

      log.info("received the state (size=" + new_state.length + " bytes)");

      Object owner = getOwnerForLock();
      try
      {
         // Acquire a lock on the root node
         acquireLocksForStateTransfer(targetRoot, owner, state_fetch_timeout,
                 true, true);

         // 1. Unserialize the states into transient and persistent state
         StateTransferIntegrator integrator =
                 StateTransferFactory.getStateTransferIntegrator(new_state,
                         targetRoot.getFqn(),
                         this);

         // 2. If transient state is available, integrate it
         try
         {
            integrator.integrateTransientState(targetRoot, cl);
            notifyAllNodesCreated(targetRoot);
         }
         catch (Throwable t)
         {
            log.error("failed setting transient state", t);
         }

         // 3. Store any persistent state
         integrator.integratePersistentState();
      }
      finally
      {
         releaseStateTransferLocks(targetRoot, owner, true);
      }

   }

   /**
    * Returns the replication version.
    */
   public String getReplicationVersion()
   {
      return repl_version_string;
   }

   /**
    * Sets the replication version from a string.
    *
    * @see Version#getVersionShort
    */
   public void setReplicationVersion(String versionString)
   {
      short version = Version.getVersionShort(versionString);
      this.replication_version = version;
      // Hold onto the string, so in case they passed in 1.0.1.RC1,
      // they can get back RC1 instead of 1.0.1.GA
      this.repl_version_string = versionString;

      // If we're are using 123 or earlier, Fqn externalization
      // should be 123 compatible as well
      // TODO find a better way to do this than setting a static variable
      if (Version.isBefore124(version))
      {
         Fqn.REL_123_COMPATIBLE = true;
      }
   }

   /**
    * Returns the replication version as a short.
    */
   public short getReplicationVersionShort()
   {
      return replication_version;
   }

   /**
    * Calls {@link #getReplicationVersionShort}.
    *
    * @deprecated
    */
   public short getStateTransferVersion()
   {
      // This method is deprecated; just call the correct method
      return getReplicationVersionShort();
   }

   /**
    * Calls {@link #setReplicationVersion} with the version string
    * from {@link Version#getVersionString}.
    *
    * @deprecated
    */
   public void setStateTransferVersion(short version)
   {
      // This method is deprecated; just convert the short to a String
      // and pass it through the correct method
      setReplicationVersion(Version.getVersionString(version));
   }

   /**
    * Acquires locks on a root node for an owner for state transfer.
    */
   protected void acquireLocksForStateTransfer(DataNode root,
                                               Object lockOwner,
                                               long timeout,
                                               boolean lockChildren,
                                               boolean force)
           throws Exception
   {
      try
      {
         if (lockChildren)
            root.acquireAll(lockOwner, timeout, DataNode.LOCK_TYPE_READ);
         else
            root.acquire(lockOwner, timeout, DataNode.LOCK_TYPE_READ);
      }
      catch (TimeoutException te)
      {
         log.error("Caught TimeoutException acquiring locks on region " +
                 root.getFqn(), te);
         if (force)
         {
            // Until we have FLUSH in place, don't force locks
//            forceAcquireLock(root, lockOwner, lockChildren);
            throw te;

         }
         else
         {
            throw te;
         }
      }
   }

   /**
    * Releases all state transfer locks acquired.
    *
    * @see #acquireLocksForStateTransfer
    */
   protected void releaseStateTransferLocks(DataNode root,
                                            Object lockOwner,
                                            boolean childrenLocked)
   {
      try
      {
         if (childrenLocked)
            root.releaseAll(lockOwner);
         else
            root.release(lockOwner);
      }
      catch (Throwable t)
      {
         log.error("failed releasing locks", t);
      }
   }

   /**
    * Forcibly acquire a read lock on the given node for the given owner,
    * breaking any existing locks that prevent the read lock.  If the
    * existing lock is held by a GlobalTransaction, breaking the lock may
    * result in a rollback of the transaction.
    *
    * @param node         the node
    * @param newOwner     the new owner (usually a Thread or GlobalTransaction)
    * @param lockChildren <code>true</code> if this method should be recursively
    *                     applied to <code>node</code>'s children.
    */
   protected void forceAcquireLock(DataNode node, Object newOwner, boolean lockChildren)
   {
      IdentityLock lock = node.getLock();
      boolean acquired = lock.isOwner(newOwner);

      if (!acquired && log.isDebugEnabled())
         log.debug("Force acquiring lock on node " + node.getFqn());

      while (!acquired)
      {
         Object curOwner = null;
         boolean attempted = false;

         // Keep breaking write locks until we acquire a read lock
         // or there are no more write locks
         while (!acquired && ((curOwner = lock.getWriterOwner()) != null))
         {
            acquired = acquireLockFromOwner(node, lock, curOwner, newOwner);
            attempted = true;
         }

         // If no more write locks, but we haven't acquired, see if we
         // need to break read locks as well
         if (!acquired && isolationLevel == IsolationLevel.SERIALIZABLE)
         {
            Iterator it = lock.getReaderOwners().iterator();
            if (it.hasNext())
            {
               curOwner = it.next();
               acquired = acquireLockFromOwner(node, lock, it.next(), newOwner);
               attempted = true;
               // Don't keep iterating due to the risk of
               // ConcurrentModificationException if readers are removed
               // Just go back through our outer loop to get the next one
            }
         }

         if (!acquired && !attempted)
         {
            // We only try to acquire above if someone else has the lock.
            // Seems no one is holding a lock and it's there for the taking.
            try
            {
               acquired = node.acquire(newOwner, 1, DataNode.LOCK_TYPE_READ);
            }
            catch (Exception ignored)
            {
            }
         }
      }

      // Recursively unlock children
      if (lockChildren && node.hasChildren())
      {
         Collection children = node.getChildren().values();
         for (Iterator it = children.iterator(); it.hasNext();)
         {
            forceAcquireLock((DataNode) it.next(), newOwner, true);
         }
      }
   }

   /**
    * Attempts to acquire a read lock on <code>node</code> for
    * <code>newOwner</code>, if necessary breaking locks held by
    * <code>curOwner</code>.
    *
    * @param node     the node
    * @param lock     the lock
    * @param curOwner the current owner
    * @param newOwner the new owner
    */
   private boolean acquireLockFromOwner(DataNode node,
                                        IdentityLock lock,
                                        Object curOwner,
                                        Object newOwner)
   {
      if (log.isTraceEnabled())
         log.trace("Attempting to acquire lock for node " + node.getFqn() +
                 " from owner " + curOwner);

      boolean acquired = false;
      boolean broken = false;
      int tryCount = 0;
      int lastStatus = TransactionLockStatus.STATUS_BROKEN;

      while (!broken && !acquired)
      {
         if (curOwner instanceof GlobalTransaction)
         {
            int status = breakTransactionLock((GlobalTransaction) curOwner, lock, lastStatus, tryCount);
            if (status == TransactionLockStatus.STATUS_BROKEN)
               broken = true;
            else if (status != lastStatus)
               tryCount = 0;
            lastStatus = status;
         }
         else if (tryCount > 0)
         {
            lock.release(curOwner);
            broken = true;
         }

         if (broken && log.isTraceEnabled())
            log.trace("Broke lock for node " + node.getFqn() +
                    " held by owner " + curOwner);

         try
         {
            acquired = node.acquire(newOwner, 1, DataNode.LOCK_TYPE_READ);
         }
         catch (Exception ignore)
         {
         }

         tryCount++;
      }

      return acquired;
   }

   /**
    * Attempts to release the lock held by <code>gtx</code> by altering the
    * underlying transaction.  Different strategies will be employed
    * depending on the status of the transaction and param
    * <code>tryCount</code>.  Transaction may be rolled back or marked
    * rollback-only, or the lock may just be broken, ignoring the tx.  Makes an
    * effort to not affect the tx or break the lock if tx appears to be in
    * the process of completion; param <code>tryCount</code> is used to help
    * make decisions about this.
    * <p/>
    * This method doesn't guarantee to have broken the lock unless it returns
    * {@link TransactionLockStatus#STATUS_BROKEN}.
    *
    * @param gtx        the gtx holding the lock
    * @param lock       the lock
    * @param lastStatus the return value from a previous invocation of this
    *                   method for the same lock, or Status.STATUS_UNKNOW
    *                   for the first invocation.
    * @param tryCount   number of times this method has been called with
    *                   the same gtx, lock and lastStatus arguments. Should
    *                   be reset to 0 anytime lastStatus changes.
    * @return the current status of the Transaction associated with
    *         <code>gtx</code>, or {@link TransactionLockStatus#STATUS_BROKEN}
    *         if the lock held by gtx was forcibly broken.
    */
   private int breakTransactionLock(GlobalTransaction gtx,
                                    IdentityLock lock,
                                    int lastStatus,
                                    int tryCount)
   {
      int status = Status.STATUS_UNKNOWN;
      Transaction tx = tx_table.getLocalTransaction(gtx);
      if (tx != null)
      {
         try
         {
            status = tx.getStatus();

            if (status != lastStatus)
               tryCount = 0;

            switch (status)
            {
               case Status.STATUS_ACTIVE:
               case Status.STATUS_MARKED_ROLLBACK:
               case Status.STATUS_PREPARING:
               case Status.STATUS_UNKNOWN:
                  if (tryCount == 0)
                  {
                     if (log.isTraceEnabled())
                        log.trace("Attempting to break transaction lock held " +
                                " by " + gtx + " by rolling back local tx");
                     // This thread has to join the tx
                     tm.resume(tx);
                     try
                     {
                        tx.rollback();
                     }
                     finally
                     {
                        tm.suspend();
                     }

                  }
                  else if (tryCount > 100)
                  {
                     // Something is wrong; our initial rollback call
                     // didn't generate a valid state change; just force it
                     lock.release(gtx);
                     status = TransactionLockStatus.STATUS_BROKEN;
                  }
                  break;

               case Status.STATUS_COMMITTING:
               case Status.STATUS_ROLLING_BACK:
                  // We'll try up to 10 times before just releasing
                  if (tryCount < 10)
                     break; // let it finish
                  // fall through and release

               case Status.STATUS_COMMITTED:
               case Status.STATUS_ROLLEDBACK:
               case Status.STATUS_NO_TRANSACTION:
                  lock.release(gtx);
                  status = TransactionLockStatus.STATUS_BROKEN;
                  break;

               case Status.STATUS_PREPARED:
                  // If the tx was started here, we can still abort the commit,
                  // otherwise we are in the middle of a remote commit() call
                  // and the status is just about to change
                  if (tryCount == 0 && gtx.addr.equals(getLocalAddress()))
                  {
                     // We can still abort the commit
                     if (log.isTraceEnabled())
                        log.trace("Attempting to break transaction lock held " +
                                "by " + gtx + " by marking local tx as " +
                                "rollback-only");
                     tx.setRollbackOnly();
                     break;
                  }
                  else if (tryCount < 10)
                  {
                     // EITHER tx was started elsewhere (in which case we'll
                     // wait a bit to allow the commit() call to finish;
                     // same as STATUS_COMMITTING above)
                     // OR we marked the tx rollbackOnly above and are just
                     // waiting a bit for the status to change
                     break;
                  }

                  // fall through and release
               default:
                  lock.release(gtx);
                  status = TransactionLockStatus.STATUS_BROKEN;
            }
         }
         catch (Exception e)
         {
            log.error("Exception breaking locks held by " + gtx, e);
            lock.release(gtx);
            status = TransactionLockStatus.STATUS_BROKEN;
         }
      }
      else
      {
         // Race condition; gtx was cleared from tx_table.
         // Just double check if gtx still holds a lock
         if (gtx == lock.getWriterOwner()
                 || lock.getReaderOwners().contains(gtx))
         {
            // TODO should we throw an exception??
            lock.release(gtx);
            status = TransactionLockStatus.STATUS_BROKEN;
         }
      }

      return status;
   }

   private void removeLocksForDeadMembers(DataNode node,
                                          Vector deadMembers)
   {
      Set deadOwners = new HashSet();
      IdentityLock lock = node.getLock();
      Object owner = lock.getWriterOwner();

      if (isLockOwnerDead(owner, deadMembers))
      {
         deadOwners.add(owner);
      }

      Iterator iter = lock.getReaderOwners().iterator();
      while (iter.hasNext())
      {
         owner = iter.next();
         if (isLockOwnerDead(owner, deadMembers))
         {
            deadOwners.add(owner);
         }
      }

      for (iter = deadOwners.iterator(); iter.hasNext();)
      {
         breakTransactionLock(node, lock, (GlobalTransaction) iter.next());
      }

      // Recursively unlock children
      if (node.hasChildren())
      {
         Collection children = node.getChildren().values();
         for (Iterator it = children.iterator(); it.hasNext();)
         {
            removeLocksForDeadMembers((DataNode) it.next(), deadMembers);
         }
      }
   }

   private void breakTransactionLock(DataNode node,
                                     IdentityLock lock,
                                     GlobalTransaction gtx)
   {
      boolean broken = false;
      int tryCount = 0;
      int lastStatus = TransactionLockStatus.STATUS_BROKEN;

      while (!broken && lock.isOwner(gtx))
      {
         int status = breakTransactionLock(gtx, lock, lastStatus, tryCount);
         if (status == TransactionLockStatus.STATUS_BROKEN)
            broken = true;
         else if (status != lastStatus)
            tryCount = 0;
         lastStatus = status;

         if (broken && log.isTraceEnabled())
            log.trace("Broke lock for node " + node.getFqn() +
                    " held by owner " + gtx);

         tryCount++;
      }
   }

   private boolean isLockOwnerDead(Object owner, Vector deadMembers)
   {
      boolean result = false;
      if (owner != null && owner instanceof GlobalTransaction)
      {
         Object addr = ((GlobalTransaction) owner).getAddress();
         result = deadMembers.contains(addr);
      }
      return result;
   }

   /**
    * Method provided to JGroups by
    * {@link TreeCacheMarshaller#objectFromByteBuffer(byte[])} when
    * it receives a replication event for an Fqn that has been marked
    * as inactive.  Currently a no-op.
    * <p/>
    * inactivate(Fqn)
    */
   public void notifyCallForInactiveSubtree(String fqn)
   {
      // do nothing
      //if (log.isTraceEnabled())
      //   log.trace(getLocalAddress() + " -- received call for inactive fqn " + fqn);
   }


   protected void fetchStateOnStartup() throws Exception
   {
      long start, stop;
      isStateSet = false;
      start = System.currentTimeMillis();
      boolean rc = channel.getState(null, state_fetch_timeout);
      if (rc)
      {
         synchronized (stateLock)
         {
            while (!isStateSet)
            {
               if (setStateException != null)
                  throw setStateException;

               try
               {
                  stateLock.wait();
               }
               catch (InterruptedException iex)
               {
               }
            }
         }
         stop = System.currentTimeMillis();
         log.info("state was retrieved successfully (in " + (stop - start) + " milliseconds)");
      }
      else
      {
         // No one provided us with state. We need to find out if that's because
         // we are the coordinator. But we don't know if the viewAccepted() callback
         // has been invoked, so call determineCoordinator(), which will block until
         // viewAccepted() is called at least once         
         determineCoordinator();

         if (isCoordinator())
         {
            log.info("State could not be retrieved (we are the first member in group)");
         }
         else
         {
            throw new CacheException("Initial state transfer failed: " +
                    "Channel.getState() returned false");
         }
      }
   }

   // -----------  End Marshalling and State Transfer -----------------------

   /**
    * @param fqn fqn String name to retrieve from cache
    * @return DataNode corresponding to the fqn. Null if does not exist. No guarantees wrt replication,
    *         cache loading are given if the underlying node is modified
    */
   public Node get(String fqn) throws CacheException
   {
      return get(Fqn.fromString(fqn));
   }

   /**
    * The same as calling {@link #get(Fqn)}  except that you can pass in options for this specific method invocation.
    * {@link Option}
    *
    * @param fqn
    * @param option
    * @return
    * @throws CacheException
    */
   public DataNode get(Fqn fqn, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);

      try
      {
         return get(fqn);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }
   }

   /**
    * The same as calling get(Fqn, Object) except that you can pass in options for this specific method invocation.
    * {@link Option}
    *
    * @param fqn
    * @param option
    * @return
    * @throws CacheException
    */
   public Object get(Fqn fqn, Object key, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);
      try
      {
         return get(fqn, key);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }
   }

   /**
    * The same as calling {@link #get(Fqn,Object,boolean)} except apply options for this
    * specific method invocation.
    */
   public Object get(Fqn fqn, Object key, boolean sendNodeEvent, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);
      try
      {
         return get(fqn, key, sendNodeEvent);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }
   }

   /**
    * The same as calling {@link #remove(Fqn)} except apply options for this
    * specific method invocation.
    */
   public void remove(Fqn fqn, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);
      try
      {
         remove(fqn);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }
   }

   /**
    * The same as calling {@link #remove(Fqn,Object)} except apply options for this
    * specific method invocation.
    */
   public Object remove(Fqn fqn, Object key, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);
      try
      {
         return remove(fqn, key);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }
   }

   /**
    * The same as calling {@link #getChildrenNames(Fqn)} except apply options for this
    * specific method invocation.
    */
   public Set getChildrenNames(Fqn fqn, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);
      try
      {
         return getChildrenNames(fqn);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }

   }

   /**
    * The same as calling {@link #put(Fqn,Map)} except apply options for this
    * specific method invocation.
    */
   public void put(Fqn fqn, Map data, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);
      try
      {
         put(fqn, data);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }
   }

   /**
    * The same as calling {@link #put(Fqn,Object,Object)} except apply options for this
    * specific method invocation.
    */
   public void put(Fqn fqn, Object key, Object value, Option option) throws CacheException
   {
      getInvocationContext().setOptionOverrides(option);
      try
      {
         put(fqn, key, value);
      }
      finally
      {
         getInvocationContext().setOptionOverrides(null);
      }
   }

   /**
    * Returns a DataNode corresponding to the fully qualified name or null if
    * does not exist.
    * No guarantees wrt replication, cache loading are given if the underlying node is modified
    *
    * @param fqn name of the DataNode to retreive
    */
   public Node get(Fqn fqn) throws CacheException
   {
      MethodCall m = MethodCallFactory.create(MethodDeclarations.getNodeMethodLocal, new Object[]{fqn});
      return (Node) invokeMethod(m);
   }

   /**
    * Returns the raw data of the node; called externally internally.
    */
   public Node _get(Fqn fqn) throws CacheException
   {
      return findNode(fqn);
   }

   /**
    * Returns the raw data of the node; called externally internally.
    * Note:  This may return a Map with the key {@link #UNINITIALIZED}
    * indicating the node was not completely loaded.
    */
   public Map _getData(Fqn fqn)
   {
      DataNode n = findNode(fqn);
      if (n == null) return null;
      return n.getData();
   }

   /**
    * Returns a set of attribute keys for the Fqn.
    * Returns null if the node is not found, otherwise a Set.
    * The set is a copy of the actual keys for this node.
    *
    * @param fqn name of the node
    */
   public Set getKeys(String fqn) throws CacheException
   {
      return getKeys(Fqn.fromString(fqn));
   }

   /**
    * Returns a set of attribute keys for the Fqn.
    * Returns null if the node is not found, otherwise a Set.
    * The set is a copy of the actual keys for this node.
    *
    * @param fqn name of the node
    */
   public Set getKeys(Fqn fqn) throws CacheException
   {
      MethodCall m = MethodCallFactory.create(MethodDeclarations.getKeysMethodLocal, new Object[]{fqn});
      return (Set) invokeMethod(m);
   }


   public Set _getKeys(Fqn fqn) throws CacheException
   {
      DataNode n = findNode(fqn);
      if (n == null)
         return null;
      Set keys = n.getDataKeys();
      // See http://jira.jboss.com/jira/browse/JBCACHE-551
      if (keys == null)
         return new HashSet(0);
      return new HashSet(keys);
   }

   /**
    * Finds a node given its name and returns the value associated with a given key in its <code>data</code>
    * map. Returns null if the node was not found in the tree or the key was not found in the hashmap.
    *
    * @param fqn The fully qualified name of the node.
    * @param key The key.
    */
   public Object get(String fqn, Object key) throws CacheException
   {
      return get(Fqn.fromString(fqn), key);
   }


   /**
    * Finds a node given its name and returns the value associated with a given key in its <code>data</code>
    * map. Returns null if the node was not found in the tree or the key was not found in the hashmap.
    *
    * @param fqn The fully qualified name of the node.
    * @param key The key.
    */
   public Object get(Fqn fqn, Object key) throws CacheException
   {
      return get(fqn, key, true);
   }

   public Object _get(Fqn fqn, Object key, boolean sendNodeEvent) throws CacheException
   {
      if (log.isTraceEnabled())
         log.trace(new StringBuffer("_get(").append("\"").append(fqn).append("\", ").append(key).append(", \"").
                 append(sendNodeEvent).append("\")"));
      DataNode n = findNode(fqn);
      if (n == null) return null;
      if (sendNodeEvent)
         notifyNodeVisited(fqn);
      return n.get(key);
   }


   protected Object get(Fqn fqn, Object key, boolean sendNodeEvent) throws CacheException
   {
      MethodCall m = MethodCallFactory.create(MethodDeclarations.getKeyValueMethodLocal, new Object[]{fqn, key, Boolean.valueOf(sendNodeEvent)});
      return invokeMethod(m);
   }

   /**
    * Like <code>get()</code> method but without triggering a node visit event. This is used
    * to prevent refresh of the cache data in the eviction policy.
    *
    * @param fqn
    * @param key
    * @deprecated This will go away.
    */
   public Object peek(Fqn fqn, Object key) throws CacheException
   {
      return get(fqn, key, false);
   }


   /**
    * added so one can get nodes internally without triggering stuff
    *
    * @deprecated This will go away.
    */
   public DataNode peek(Fqn fqn)
   {
      return findInternal(fqn, true);
   }

   /**
    * Checks whether a given node exists in current in-memory state of the tree.
    * Does not acquire any locks in doing so (result may be dirty read). Does
    * not attempt to load nodes from a cache loader (may return false if a
    * node has been evicted).
    *
    * @param fqn The fully qualified name of the node
    * @return boolean Whether or not the node exists
    */
   public boolean exists(String fqn)
   {
      return exists(Fqn.fromString(fqn));
   }


   /**
    * Checks whether a given node exists in current in-memory state of the tree.
    * Does not acquire any locks in doing so (result may be dirty read). Does
    * not attempt to load nodes from a cache loader (may return false if a
    * node has been evicted).
    *
    * @param fqn The fully qualified name of the node
    * @return boolean Whether or not the node exists
    */
   public boolean exists(Fqn fqn)
   {
      DataNode n = findInternal(fqn, false);
      return n != null;
   }

   /**
    * Gets node without attempt to load it from CacheLoader if not present
    *
    * @param fqn
    */
   private Node findInternal(Fqn fqn, boolean includeNodesMarkedAsRemoved)
   {
      if (fqn == null || fqn.size() == 0) return (Node) root;
      TreeNode n = root;
      int fqnSize = fqn.size();
      for (int i = 0; i < fqnSize; i++)
      {
         Object obj = fqn.get(i);
         n = n.getChild(obj);
         if (n == null)
            return null;
         else if (!includeNodesMarkedAsRemoved && ((DataNode) n).isMarkedForRemoval())
            return null;
      }
      return (Node) n;
   }


   /**
    * @param fqn
    * @param key
    */
   public boolean exists(String fqn, Object key)
   {
      return exists(Fqn.fromString(fqn), key);
   }


   /**
    * Checks whether a given key exists in the given node. Does not interact with CacheLoader, so the behavior is
    * different from {@link #get(Fqn,Object)}
    *
    * @param fqn The fully qualified name of the node
    * @param key
    * @return boolean Whether or not the node exists
    */
   public boolean exists(Fqn fqn, Object key)
   {
      DataNode n = findInternal(fqn, false);
      return n != null && n.containsKey(key);
   }


   /**
    * Adds a new node to the tree and sets its data. If the node doesn not yet exist, it will be created.
    * Also, parent nodes will be created if not existent. If the node already has data, then the new data
    * will override the old one. If the node already existed, a nodeModified() notification will be generated.
    * Otherwise a nodeCreated() motification will be emitted.
    *
    * @param fqn  The fully qualified name of the new node
    * @param data The new data. May be null if no data should be set in the node.
    */
   public void put(String fqn, Map data) throws CacheException
   {
      put(Fqn.fromString(fqn), data);
   }

   /**
    * Sets a node's data. If the node does not yet exist, it will be created.
    * Also, parent nodes will be created if not existent. If the node already has data, then the new data
    * will override the old one. If the node already existed, a nodeModified() notification will be generated.
    * Otherwise a nodeCreated() motification will be emitted.
    *
    * @param fqn  The fully qualified name of the new node
    * @param data The new data. May be null if no data should be set in the node.
    */
   public void put(Fqn fqn, Map data) throws CacheException
   {
      GlobalTransaction tx = getCurrentTransaction();
      MethodCall m = MethodCallFactory.create(MethodDeclarations.putDataMethodLocal, new Object[]{tx, fqn, data, Boolean.TRUE});
      invokeMethod(m);
   }

   /**
    * Adds a key and value to a given node. If the node doesn't exist, it will be created. If the node
    * already existed, a nodeModified() notification will be generated. Otherwise a
    * nodeCreated() motification will be emitted.
    *
    * @param fqn   The fully qualified name of the node
    * @param key   The key
    * @param value The value
    * @return Object The previous value (if any), if node was present
    */
   public Object put(String fqn, Object key, Object value) throws CacheException
   {
      return put(Fqn.fromString(fqn), key, value);
   }


   /**
    * Put with the following properties:
    * <ol>
    * <li>Fails fast (after timeout milliseconds)
    * <li>If replication is used: replicates <em>asynchronously</em>, overriding a potential synchronous mode
    * </ol>
    * This method should be used without running in a transaction (suspend()/resume() before calling it)
    *
    * @param fqn     The fully qualified name of the node
    * @param key
    * @param value
    * @param timeout Number of milliseconds to wait until a lock has been acquired. A TimeoutException will
    *                be thrown if not successful. 0 means to wait forever
    * @return
    * @throws CacheException
    * @deprecated This is a kludge created specifically form the Hibernate 3.0 release. This method should
    *             <em>not</em> be used by any application. The methodV will likely be removed in a future release
    */
   public Object putFailFast(Fqn fqn, Object key, Object value, long timeout) throws CacheException
   {
      if (isNodeLockingOptimistic()) throw new UnsupportedOperationException("putFailFast() is not supported with Optimistic Locking");
      GlobalTransaction tx = getCurrentTransaction();
      MethodCall m = MethodCallFactory.create(MethodDeclarations.putFailFastKeyValueMethodLocal,
              new Object[]{tx, fqn, key, value, Boolean.TRUE, new Long(timeout)});
      return invokeMethod(m);
   }

   /**
    * @param fqn
    * @param key
    * @param value
    * @param timeout
    * @return
    * @throws CacheException
    * @deprecated
    */
   public Object putFailFast(String fqn, Object key, Object value, long timeout) throws CacheException
   {
      return putFailFast(Fqn.fromString(fqn), key, value, timeout);
   }

   /**
    * Adds a key and value to a given node. If the node doesn't exist, it will be created. If the node
    * already existed, a nodeModified() notification will be generated. Otherwise a
    * nodeCreated() motification will be emitted.
    *
    * @param fqn   The fully qualified name of the node
    * @param key   The key
    * @param value The value
    * @return Object The previous value (if any), if node was present
    */
   public Object put(Fqn fqn, Object key, Object value) throws CacheException
   {
      GlobalTransaction tx = getCurrentTransaction();
      MethodCall m = MethodCallFactory.create(MethodDeclarations.putKeyValMethodLocal, new Object[]{tx, fqn, key, value, Boolean.TRUE});
      return invokeMethod(m);
   }

   /**
    * Removes the node from the tree.
    *
    * @param fqn The fully qualified name of the node.
    */
   public void remove(String fqn) throws CacheException
   {
      remove(Fqn.fromString(fqn));
   }

   /**
    * Removes the node from the tree.
    *
    * @param fqn The fully qualified name of the node.
    */
   public void remove(Fqn fqn) throws CacheException
   {
      GlobalTransaction tx = getCurrentTransaction();
      if (fqn.isRoot())
      {
         // special treatment for removal of root node - just remove all children
         Set children = _getChildrenNames(fqn);
         // we need to preserve options
         Option o = getInvocationContext().getOptionOverrides();
         if (children != null)
         {
            for (Iterator i = children.iterator(); i.hasNext();)
            {
               Fqn childFqn = new Fqn(fqn, i.next());
               if (!internalFqns.contains(childFqn)) remove(childFqn, o);
            }
         }
      }
      else
      {
         MethodCall m = MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal, new Object[]{tx, fqn, Boolean.TRUE});
         invokeMethod(m);
      }
   }

   /**
    * Called by eviction policy provider. Note that eviction is done only in
    * local mode, that is, it doesn't replicate the node removal. This will
    * cause the replication nodes to not be synchronizing, which is fine since
    * the value will be fetched again when {@link #get} returns null. After
    * that, the contents will be in sync.
    *
    * @param fqn Will remove everythign assoicated with this fqn.
    * @throws CacheException
    */
   public void evict(Fqn fqn) throws CacheException
   {
      if (fqn.isRoot())
      {
         // special treatment for removal of root node - just remove all children
         Set children = _getChildrenNames(fqn);
         if (children != null)
         {
            for (Iterator i = children.iterator(); i.hasNext();)
            {
               Fqn childFqn = new Fqn(fqn, i.next());
               if (!internalFqns.contains(childFqn)) evict(childFqn);
            }
         }
      }
      else
      {
         MethodCall m = MethodCallFactory.create(MethodDeclarations.evictNodeMethodLocal, new Object[]{fqn});
         invokeMethod(m);
      }
   }

   /**
    * Removes <code>key</code> from the node's hashmap
    *
    * @param fqn The fullly qualified name of the node
    * @param key The key to be removed
    * @return The previous value, or null if none was associated with the given key
    */
   public Object remove(String fqn, Object key) throws CacheException
   {
      return remove(Fqn.fromString(fqn), key);
   }

   /**
    * Removes <code>key</code> from the node's hashmap
    *
    * @param fqn The fullly qualified name of the node
    * @param key The key to be removed
    * @return The previous value, or null if none was associated with the given key
    */
   public Object remove(Fqn fqn, Object key) throws CacheException
   {
      GlobalTransaction tx = getCurrentTransaction();
      MethodCall m = MethodCallFactory.create(MethodDeclarations.removeKeyMethodLocal, new Object[]{tx, fqn, key, Boolean.TRUE});
      return invokeMethod(m);
   }

   /**
    * Removes the keys and properties from a node.
    */
   public void removeData(String fqn) throws CacheException
   {
      removeData(Fqn.fromString(fqn));
   }

   /**
    * Removes the keys and properties from a named node.
    */
   public void removeData(Fqn fqn) throws CacheException
   {
      GlobalTransaction tx = getCurrentTransaction();
      MethodCall m = MethodCallFactory.create(MethodDeclarations.removeDataMethodLocal, new Object[]{tx, fqn, Boolean.TRUE});
      invokeMethod(m);
   }

   /**
    * Lock a given node (or the entire subtree starting at this node)
    * @param fqn The FQN of the node
    * @param owner The owner. This is simply a key into a hashtable, and can be anything, e.g.
    * a GlobalTransaction, the current thread, or a special object. If null, it is set to Thread.currentThread()
    * @param lock_type The type of lock (RO, RW). Needs to be of type DataNode.LOCK_TYPE_READ or DataNode.LOCK_TYPE_WRITE
    * @param lock_recursive If true, the entire subtree is locked, else only the given node
    * @throws CacheException If node doesn't exist, a NodeNotExistsException is throw. Other exceptions are
    * LockingException, TimeoutException and UpgradeException
    */
//   public void lock(Fqn fqn, Object owner, int lock_type, boolean lock_recursive) throws CacheException {
//
//   }

   /**
    * Unlock a given node (or the entire subtree starting at this node)
    * @param fqn The FQN of the node
    * @param owner The owner. This is simply a key into a hashtable, and can be anything, e.g.
    * a GlobalTransaction, the current thread, or a special object. If null, it is set to Thread.currentThread()
    * @param unlock_recursive If true, the entire subtree is unlocked, else only the given node
    * @param force Release the lock even if we're not the owner
    */
//   public void unlock(Fqn fqn, Object owner, boolean unlock_recursive, boolean force) {
//
//   }

   /**
    * Releases all locks for this node and the entire node subtree.
    */
   public void releaseAllLocks(String fqn)
   {
      releaseAllLocks(Fqn.fromString(fqn));
   }

   /**
    * Releases all locks for this node and the entire node subtree.
    */
   public void releaseAllLocks(Fqn fqn)
   {
      MethodCall m = MethodCallFactory.create(MethodDeclarations.releaseAllLocksMethodLocal, new Object[]{fqn});
      try
      {
         invokeMethod(m);
      }
      catch (CacheException e)
      {
         log.error("failed releasing all locks for " + fqn, e);
      }
   }

   /**
    * Prints a representation of the node defined by <code>fqn</code>.
    * Output includes name, fqn and data.
    */
   public String print(String fqn)
   {
      return print(Fqn.fromString(fqn));
   }

   /**
    * Prints a representation of the node defined by <code>fqn</code>.
    * Output includes name, fqn and data.
    */
   public String print(Fqn fqn)
   {
      MethodCall m = MethodCallFactory.create(MethodDeclarations.printMethodLocal, new Object[]{fqn});
      Object retval = null;
      try
      {
         retval = invokeMethod(m);
      }
      catch (Throwable e)
      {
         retval = e;
      }
      if (retval != null)
         return retval.toString();
      else return "";
   }


   /**
    * Returns all children of a given node.
    * Returns null of the parent node was not found, or if there are no
    * children.
    * The set is unmodifiable.
    *
    * @param fqn The fully qualified name of the node
    * @return Set A list of child names (as Strings)
    * @see #getChildrenNames(Fqn)
    */
   public Set getChildrenNames(String fqn) throws CacheException
   {
      return getChildrenNames(Fqn.fromString(fqn));
   }

   /**
    * Returns all children of a given node.
    * Returns null of the parent node was not found, or if there are no
    * children.
    * The set is unmodifiable.
    *
    * @param fqn The fully qualified name of the node
    * @return Set an unmodifiable set of children names, Object.
    */
   public Set getChildrenNames(Fqn fqn) throws CacheException
   {
      MethodCall m = MethodCallFactory.create(MethodDeclarations.getChildrenNamesMethodLocal, new Object[]{fqn});
      return (Set) invokeMethod(m);
   }

   public Set _getChildrenNames(Fqn fqn) throws CacheException
   {
      DataNode n = findNode(fqn);
      if (n == null) return null;
      Map m = n.getChildren();
      if (m != null)
      {
         return new HashSet(m.keySet());
      }
      else
         return null;
   }


   public boolean hasChild(Fqn fqn)
   {
      if (fqn == null) return false;

      TreeNode n = root;
      Object obj;
      for (int i = 0; i < fqn.size(); i++)
      {
         obj = fqn.get(i);
         n = n.getChild(obj);
         if (n == null)
            return false;
      }
      return n.hasChildren();
   }

   /**
    * Returns a debug string with few details.
    */
   public String toString()
   {
      return toString(false);
   }


   /**
    * Returns a debug string with optional details of contents.
    */
   public String toString(boolean details)
   {
      StringBuffer sb = new StringBuffer();
      int indent = 0;
      Map children;

      if (!details)
      {
         sb.append(getClass().getName()).append(" [").append(getNumberOfNodes()).append(" nodes, ");
         sb.append(getNumberOfLocksHeld()).append(" locks]");
      }
      else
      {
         children = root.getChildren();
         if (children != null && children.size() > 0)
         {
            Collection nodes = children.values();
            for (Iterator it = nodes.iterator(); it.hasNext();)
            {
               ((DataNode) it.next()).print(sb, indent);
               sb.append("\n");
            }
         }
         else
            sb.append(Fqn.SEPARATOR);
      }
      return sb.toString();
   }


   /**
    * Prints information about the contents of the nodes in the tree's current
    * in-memory state.  Does not load any previously evicted nodes from a
    * cache loader, so evicted nodes will not be included.
    */
   public String printDetails()
   {
      StringBuffer sb = new StringBuffer();
      int indent = 2;
      Map children;

      children = root.getChildren();
      if (children != null && children.size() > 0)
      {
         Collection nodes = children.values();
         for (Iterator it = nodes.iterator(); it.hasNext();)
         {
            ((DataNode) it.next()).printDetails(sb, indent);
            sb.append("\n");
         }
      }
      else
         sb.append(Fqn.SEPARATOR);
      return sb.toString();
   }

   /**
    * Returns lock information.
    */
   public String printLockInfo()
   {
      StringBuffer sb = new StringBuffer("\n");
      int indent = 0;
      Map children;

      sb.append("Root lock: ");
      if (root.isLocked())
      {
         sb.append("\t(");
         root.getLock().toString(sb);
         sb.append(")");
      }
      sb.append("\n");

      children = root.getChildren();
      if (children != null && children.size() > 0)
      {
         Collection nodes = children.values();
         for (Iterator it = nodes.iterator(); it.hasNext();)
         {
            ((DataNode) it.next()).printLockInfo(sb, indent);
            sb.append("\n");
         }
      }
      else
         sb.append(Fqn.SEPARATOR);
      return sb.toString();
   }

   /**
    * Returns the number of read or write locks held across the entire tree.
    */
   public int getNumberOfLocksHeld()
   {
      return numLocks((Node) root);
   }

   private int numLocks(Node n)
   {
      int num = 0;
      Map children;
      if (n.isLocked())
         num++;
      if ((children = n.getChildren(true)) != null)
      {
         for (Iterator it = children.values().iterator(); it.hasNext();)
         {
            num += numLocks((Node) it.next());
         }
      }
      return num;
   }

   /**
    * Returns an <em>approximation</em> of the total number of nodes in the
    * tree. Since this method doesn't acquire any locks, the number might be
    * incorrect, or the method might even throw a
    * ConcurrentModificationException
    */
   public int getNumberOfNodes()
   {
      return numNodes(root) - 1;
   }

   private int numNodes(DataNode n)
   {
      if (n == null)
         return 0;
      int count = 1; // for n
      if (n.hasChildren())
      {
         Map children = n.getChildren();
         if (children != null && children.size() > 0)
         {
            Collection child_nodes = children.values();
            DataNode child;
            for (Iterator it = child_nodes.iterator(); it.hasNext();)
            {
               child = (DataNode) it.next();
               count += numNodes(child);
            }
         }
      }
      return count;
   }

   /**
    * Internal method; not to be used externally.
    *
    * @param f
    */
   public void realRemove(Fqn f, boolean skipMarkerCheck)
   {
      Node n = findInternal(f, true);
      if (n == null)
         return;

      if (log.isDebugEnabled()) log.debug("Performing a real remove for node " + f + ", marked for removal.");
      if (skipMarkerCheck || n.isMarkedForRemoval())
      {
         if (n.getFqn().isRoot())
         {
            // do not actually delete; just remove deletion marker
            n.unmarkForRemoval(true);
            // but now remove all children, since the call has been to remove("/")
            n.removeAllChildren();

         }
         else
         {
            n.getParent().removeChild(n.getName());
         }
      }
      else
      {
         if (log.isDebugEnabled()) log.debug("Node " + f + " NOT marked for removal as expected, not removing!");
      }
   }


   /**
    * Returns an <em>approximation</em> of the total number of attributes in
    * the tree. Since this method doesn't acquire any locks, the number might
    * be incorrect, or the method might even throw a
    * ConcurrentModificationException
    */
   public int getNumberOfAttributes()
   {
      return numAttributes(root);
   }

   /**
    * Returns an <em>approximation</em> of the total number of attributes in
    * this sub tree.
    *
    * @see #getNumberOfAttributes
    */
   public int getNumberOfAttributes(Fqn fqn)
   {
      DataNode n = findNode(fqn);
      return numAttributes(n);
   }

   private int numAttributes(DataNode n)
   {
      if (n == null)
         return 0;
      int count = n.numAttributes();
      if (n.hasChildren())
      {
         Map children = n.getChildren();
         if (children != null && children.size() > 0)
         {
            Collection child_nodes = children.values();
            DataNode child;
            for (Iterator it = child_nodes.iterator(); it.hasNext();)
            {
               child = (DataNode) it.next();
               count += numAttributes(child);
            }
         }
      }
      return count;
   }

   /* ---------------------- Remote method calls -------------------- */

   /**
    * @param mbrs
    * @param method_call
    * @param synchronous
    * @param exclude_self
    * @param timeout
    * @return
    * @throws Exception
    * @deprecated Note this is due to be moved to an interceptor.
    */
   public List callRemoteMethods(List mbrs, MethodCall method_call,
                                 boolean synchronous, boolean exclude_self, long timeout)
           throws Exception
   {
      return callRemoteMethods(mbrs, method_call, synchronous ? GroupRequest.GET_ALL : GroupRequest.GET_NONE, exclude_self, timeout);
   }


   /**
    * Overloaded to allow a finer grained control over JGroups mode
    *
    * @param mbrs
    * @param method_call
    * @param mode
    * @param exclude_self
    * @param timeout
    * @return
    * @throws Exception
    * @deprecated Note this is due to be moved to an interceptor.
    */
   public List callRemoteMethods(List mbrs, MethodCall method_call, int mode, boolean exclude_self, long timeout)
           throws Exception
   {
      RspList rsps;
      Rsp rsp;
      List retval;
      Vector validMembers;

      if (disp == null)
         return null;

      validMembers = mbrs != null ? new Vector(mbrs) : new Vector(this.members);
      if (exclude_self && validMembers.size() > 0)
      {
         Object local_addr = getLocalAddress();
         if (local_addr != null)
            validMembers.remove(local_addr);
      }
      if (validMembers.size() == 0)
      {
         if (log.isTraceEnabled())
            log.trace("destination list is empty, discarding call");
         return null;
      }

      if (log.isTraceEnabled())
         log.trace("callRemoteMethods(): valid members are " + validMembers + " method: " + method_call);

      // if we are using buddy replication, all calls should use ANYCAST.  Otherwise, use the default (multicast).  Unless anycast is forced

//      rsps = callRemoteMethodsViaReflection(validMembers, method_call, mode, timeout, false);//forceAnycast || buddyManager != null && buddyManager.isEnabled());
      rsps = callRemoteMethodsViaReflection(validMembers, method_call, mode, timeout, forceAnycast || buddyManager != null && buddyManager.isEnabled());

      // a null response is 99% likely to be due to a marshalling problem - we throw a NSE, this needs to be changed when
      // JGroups supports http://jira.jboss.com/jira/browse/JGRP-193
      if (rsps == null)
      {
         // return null;
         throw new NotSerializableException("RpcDispatcher returned a null.  This is most often caused by args for " + method_call.getName() + " not being serializable.");
      }

      if (mode == GroupRequest.GET_NONE)
         return new ArrayList(); // async case

      if (log.isTraceEnabled())
         log.trace("(" + getLocalAddress() + "): responses for method " + method_call.getName() + ":\n" + rsps);

      retval = new ArrayList(rsps.size());
      for (int i = 0; i < rsps.size(); i++)
      {
         rsp = (Rsp) rsps.elementAt(i);
         if (rsp.wasSuspected() || !rsp.wasReceived())
         {
            CacheException ex;
            if (rsp.wasSuspected())
            {
               ex = new SuspectException("Response suspected: " + rsp);
            }
            else
            {
               ex = new TimeoutException("Response timed out: " + rsp);
            }
            retval.add(new ReplicationException("rsp=" + rsp, ex));
         }
         else
         {
            if (rsp.getValue() instanceof Exception)
            {
               if (log.isTraceEnabled()) log.trace("Recieved exception'" + rsp.getValue() + "' from " + rsp.getSender());
               throw (Exception) rsp.getValue();
            }
            retval.add(rsp.getValue());
         }
      }
      return retval;
   }

   private RspList callRemoteMethodsViaReflection(Vector validMembers, MethodCall method_call, int mode, long timeout, boolean anycast) throws Exception
   {
      if (anycast && anycastMethod != null)
      {
         // Using reflection since we need JGroups >= 2.4.1 for this.
         return (RspList) anycastMethod.invoke(disp, new Object[]{validMembers, method_call, new Integer(mode), new Long(timeout), Boolean.valueOf(anycast)});
      }
      else return disp.callRemoteMethods(validMembers, method_call, mode, timeout);
   }

   /**
    * @param members
    * @param method
    * @param args
    * @param synchronous
    * @param exclude_self
    * @param timeout
    * @return
    * @throws Exception
    * @deprecated Note this is due to be moved to an interceptor.
    */
   public List callRemoteMethods(List members, Method method, Object[] args,
                                 boolean synchronous, boolean exclude_self, long timeout)
           throws Exception
   {
      return callRemoteMethods(members, MethodCallFactory.create(method, args), synchronous, exclude_self, timeout);
   }

   public List callRemoteMethods(Vector members, Method method, Object[] args,
                                 boolean synchronous, boolean exclude_self, long timeout)
           throws Exception
   {
      return callRemoteMethods(members, MethodCallFactory.create(method, args), synchronous, exclude_self, timeout);
   }

   /**
    * @param members
    * @param method_name
    * @param types
    * @param args
    * @param synchronous
    * @param exclude_self
    * @param timeout
    * @return
    * @throws Exception
    * @deprecated Note this is due to be moved to an interceptor.
    */
   public List callRemoteMethods(Vector members, String method_name,
                                 Class[] types, Object[] args,
                                 boolean synchronous, boolean exclude_self, long timeout)
           throws Exception
   {
      Method method = getClass().getDeclaredMethod(method_name, types);
      return callRemoteMethods(members, method, args, synchronous, exclude_self, timeout);
   }
   /* -------------------- End Remote method calls ------------------ */

   /* --------------------- Callbacks -------------------------- */

   /* ----- These are VERSIONED callbacks to facilitate JBCACHE-843.  Also see docs/design/DataVersion.txt --- */

   public void _put(GlobalTransaction tx, Fqn fqn, Map data, boolean create_undo_ops, DataVersion dv) throws CacheException
   {
      _put(tx, fqn, data, create_undo_ops, false, dv);
   }

   public void _put(GlobalTransaction tx, Fqn fqn, Map data, boolean create_undo_ops, boolean erase_contents, DataVersion dv) throws CacheException
   {
      _put(tx, fqn, data, create_undo_ops, erase_contents);
   }

   public Object _put(GlobalTransaction tx, Fqn fqn, Object key, Object value, boolean create_undo_ops, DataVersion dv) throws CacheException
   {
      return _put(tx, fqn, key, value, create_undo_ops);
   }

   public void _remove(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, DataVersion dv) throws CacheException
   {
      _remove(tx, fqn, create_undo_ops, true);
   }

   public Object _remove(GlobalTransaction tx, Fqn fqn, Object key, boolean create_undo_ops, DataVersion dv) throws CacheException
   {
      return _remove(tx, fqn, key, create_undo_ops);
   }

   public void _removeData(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, DataVersion dv) throws CacheException
   {

      _removeData(tx, fqn, create_undo_ops, true);
   }

   /* ----- End VERSIONED callbacks - Now for the NORMAL callbacks. -------- */


   /**
    * Internal put method.
    * Does the real work. Needs to acquire locks if accessing nodes, depending on
    * the value of <tt>locking</tt>. If run inside a transaction, needs to (a) add
    * newly acquired locks to {@link TransactionEntry}'s lock list, (b) add nodes
    * that were created to {@link TransactionEntry}'s node list and (c) create
    * {@link Modification}s and add them to {@link TransactionEntry}'s modification
    * list and (d) create compensating modifications to undo the changes in case
    * of a rollback
    *
    * @param fqn
    * @param data
    * @param create_undo_ops If true, undo operations will be created (default is true).
    *                        Otherwise they will not be created (used by rollback()).
    */
   public void _put(GlobalTransaction tx, String fqn, Map data, boolean create_undo_ops)
           throws CacheException
   {
      _put(tx, Fqn.fromString(fqn), data, create_undo_ops);
   }


   /**
    * Internal put method.
    * Does the real work. Needs to acquire locks if accessing nodes, depending on
    * the value of <tt>locking</tt>. If run inside a transaction, needs to (a) add
    * newly acquired locks to {@link TransactionEntry}'s lock list, (b) add nodes
    * that were created to {@link TransactionEntry}'s node list and (c) create
    * {@link Modification}s and add them to {@link TransactionEntry}'s modification
    * list and (d) create compensating modifications to undo the changes in case
    * of a rollback
    *
    * @param fqn
    * @param data
    * @param create_undo_ops If true, undo operations will be created (default is true).
    *                        Otherwise they will not be created (used by rollback()).
    */
   public void _put(GlobalTransaction tx, Fqn fqn, Map data, boolean create_undo_ops)
           throws CacheException
   {
      _put(tx, fqn, data, create_undo_ops, false);
   }

   /**
    * Internal put method.
    * Does the real work. Needs to acquire locks if accessing nodes, depending on
    * the value of <tt>locking</tt>. If run inside a transaction, needs to (a) add
    * newly acquired locks to {@link TransactionEntry}'s lock list, (b) add nodes
    * that were created to {@link TransactionEntry}'s node list and (c) create
    * {@link Modification}s and add them to {@link TransactionEntry}'s modification
    * list and (d) create compensating modifications to undo the changes in case
    * of a rollback
    *
    * @param fqn
    * @param data
    * @param create_undo_ops If true, undo operations will be created (default is true).
    * @param erase_contents  Clear the existing hashmap before putting the new data into it
    *                        Otherwise they will not be created (used by rollback()).
    */
   public void _put(GlobalTransaction tx, Fqn fqn, Map data, boolean create_undo_ops, boolean erase_contents)
           throws CacheException
   {
      Node n;
      MethodCall undo_op = null;
      Map old_data;

      if (log.isTraceEnabled())
      {
         log.trace(new StringBuffer("_put(").append(tx).append(", \"").append(fqn).append("\", ").append(data).append(")"));
      }

      // Find the node. This will lock it (if <tt>locking</tt> is true) and
      // add the temporarily created parent nodes to the TX's node list if tx != null)
      n = findNode(fqn);
      if (n == null)
      {
         String errStr = "node " + fqn + " not found (gtx=" + tx + ", caller=" + Thread.currentThread() + ")";
         if (log.isTraceEnabled())
            log.trace(errStr);
         throw new NodeNotExistsException(errStr);
      }
      notifyNodeModify(fqn, true);

      n.unmarkForRemoval(false);

      // TODO: move creation of undo-operations to separate Interceptor
      // create a compensating method call (reverting the effect of
      // this modification) and put it into the TX's undo list.
      if (tx != null && create_undo_ops)
      {
         // TODO even if n is brand new, getData can have empty value instead. Need to fix.
         if ((old_data = n.getData()) == null)
         {
            undo_op = MethodCallFactory.create(MethodDeclarations.removeDataMethodLocal,
                    new Object[]{tx, fqn, Boolean.FALSE});
         }
         else
         {
            undo_op = MethodCallFactory.create(MethodDeclarations.putDataEraseMethodLocal,
                    new Object[]{tx, fqn,
                            new HashMap(old_data),
                            Boolean.FALSE,
                            Boolean.TRUE}); // erase previous hashmap contents
         }
      }

      n.put(data, erase_contents);

      if (tx != null && create_undo_ops)
      {
         // 1. put undo-op in TX' undo-operations list (needed to rollback TX)
         tx_table.addUndoOperation(tx, undo_op);
      }
      notifyNodeModified(fqn);
      notifyNodeModify(fqn, false);
   }

   /**
    * Internal put method.
    *
    * @return Previous value (if any)
    */
   public Object _put(GlobalTransaction tx, String fqn, Object key, Object value, boolean create_undo_ops)
           throws CacheException
   {
      return _put(tx, Fqn.fromString(fqn), key, value, create_undo_ops);
   }


   /**
    * Internal put method.
    *
    * @return Previous value (if any)
    */
   public Object _put(GlobalTransaction tx, Fqn fqn, Object key, Object value, boolean create_undo_ops)
           throws CacheException
   {
      Node n = null;
      MethodCall undo_op = null;
      Object old_value = null;

      if (log.isTraceEnabled())
      {
         log.trace(new StringBuffer("_put(").append(tx).append(", \"").
                 append(fqn).append("\", ").append(key).append(", ").append(value).append(")"));
      }

      n = findNode(fqn);
      if (n == null)
      {
         String errStr = "node " + fqn + " not found (gtx=" + tx + ", caller=" + Thread.currentThread() + ")";
         if (log.isTraceEnabled())
            log.trace(errStr);
         throw new NodeNotExistsException(errStr);
      }

      notifyNodeModify(fqn, true);
      old_value = n.put(key, value);

      n.unmarkForRemoval(false);

      // create a compensating method call (reverting the effect of
      // this modification) and put it into the TX's undo list.
      if (tx != null && create_undo_ops)
      {
         if (old_value == null)
         {
            undo_op = MethodCallFactory.create(MethodDeclarations.removeKeyMethodLocal,
                    new Object[]{tx, fqn, key, Boolean.FALSE});
         }
         else
         {
            undo_op = MethodCallFactory.create(MethodDeclarations.putKeyValMethodLocal,
                    new Object[]{tx, fqn, key, old_value,
                            Boolean.FALSE});
         }
         // 1. put undo-op in TX' undo-operations list (needed to rollback TX)
         tx_table.addUndoOperation(tx, undo_op);
      }

      notifyNodeModified(fqn);
      notifyNodeModify(fqn, false);
      return old_value;
   }

   /**
    * Internal put method.
    */
   public Object _put(GlobalTransaction tx, Fqn fqn, Object key, Object value, boolean create_undo_ops, long timeout)
           throws CacheException
   {
      return _put(tx, fqn, key, value, create_undo_ops);
   }

   /**
    * Internal remove method.
    */
   public void _remove(GlobalTransaction tx, String fqn, boolean create_undo_ops) throws CacheException
   {
      _remove(tx, Fqn.fromString(fqn), create_undo_ops);
   }

   /**
    * Internal remove method.
    */
   public void _remove(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops) throws CacheException
   {
      _remove(tx, fqn, create_undo_ops, true);
   }

   public void _remove(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, boolean sendNodeEvent)
           throws CacheException
   {
      _remove(tx, fqn, create_undo_ops, sendNodeEvent, false);
   }

   /**
    * Internal method to remove a node.
    *
    * @param tx
    * @param fqn
    * @param create_undo_ops
    * @param sendNodeEvent
    */
   public void _remove(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, boolean sendNodeEvent, boolean eviction)
           throws CacheException
   {
      _remove(tx, fqn, create_undo_ops, sendNodeEvent, eviction, null);
   }

   /**
    * Internal method to remove a node.
    * Performs a remove on a node, passing in a {@link DataVersion} which is used with optimistically locked nodes.  Pass
    * in a null if optimistic locking is not used.
    *
    * @param tx
    * @param fqn
    * @param create_undo_ops
    * @param sendNodeEvent
    * @param eviction
    * @param version
    * @throws CacheException
    */
   public void _remove(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, boolean sendNodeEvent, boolean eviction, DataVersion version)
           throws CacheException
   {

      Node n;
      TreeNode parent_node;
      MethodCall undo_op = null;

      // Users should never see this exception - it is there to trap dev bugs where remove or evict calls on the root node
      // make it all the way down here.  They should be dealt with at a higher level, such as remove() or evict().
      if (fqn.isRoot()) throw new RuntimeException("Attempting to remove or evict the root node.  This is not supported in TreeCache._remove() and should be dealt with at a higher level.");

      if (log.isTraceEnabled())
         log.trace(new StringBuffer("_remove(").append(tx).append(", \"").append(fqn).append("\")"));

      // check if this is triggered by a rollback operation ...
      if (tx != null)
      {
         try
         {
            int status = tx_table.getLocalTransaction(tx).getStatus();
            if (status == Status.STATUS_MARKED_ROLLBACK || status == Status.STATUS_ROLLEDBACK || status == Status.STATUS_ROLLING_BACK)
            {
               if (log.isDebugEnabled())
                  log.debug("This remove call is triggered by a transaction rollback, as a compensation operation.  Do a realRemove() instead.");
               realRemove(fqn, true);
               return;
            }
         }
         catch (Exception e)
         {
            // what do we do here?
            log.warn("Unable to get a hold of the current transaction for a supposedly transactional call.  This may result in stale locks!", e);

         }
      }

      // removing the root node - should never get here!
      /*
      if (fqn.size() == 0)
      {
         Set children = getChildrenNames(fqn);
         if (children != null)
         {
            Object[] kids = children.toArray();

            for (int i = 0; i < kids.length; i++)
            {
               Object s = kids[i];
               Fqn tmp = new Fqn(fqn, s);
               try
               {
                  _remove(tx, tmp, create_undo_ops, true, eviction);
               }
               catch (Exception e)
               {
                  log.error("failure removing node " + tmp);
               }
            }
         }
         return;
      }
      */

      // Find the node. This will add the temporarily created parent nodes to the TX's node list if tx != null)
      n = findNode(fqn, version);
      if (n == null)
      {
         if (log.isTraceEnabled())
            log.trace("node " + fqn + " not found");
         return;
      }
      if (sendNodeEvent)
         notifyNodeRemove(fqn, true);
      else
         notifyNodeEvict(fqn, true);

      parent_node = n.getParent();

      if (isNodeLockingOptimistic() || eviction)
         parent_node.removeChild(n.getName());
      else
         n.markForRemoval();

      if (eviction)
         parent_node.setChildrenLoaded(false);

      // release all locks for the entire subtree
      // JBCACHE-871 -- this is not correct!  This is the lock interceptor's task
      // n.releaseAll(tx != null ? tx : (Object) Thread.currentThread());

      // create a compensating method call (reverting the effect of
      // this modification) and put it into the TX's undo list.
      if (tx != null && create_undo_ops && !eviction)
      {
         undo_op = MethodCallFactory.create(MethodDeclarations.addChildMethodLocal, new Object[]{tx, parent_node.getFqn(), n.getName(), n});

         // 1. put undo-op in TX' undo-operations list (needed to rollback TX)
         tx_table.addUndoOperation(tx, undo_op);
      }

      if (sendNodeEvent)
      {
         notifyNodeRemoved(fqn);
         notifyNodeRemove(fqn, false);
      }
      else
      {
         notifyNodeEvicted(fqn);
         notifyNodeEvict(fqn, false);
      }
   }

   /**
    * Internal method to remove a key.
    *
    * @param fqn
    * @param key
    * @return Object
    */
   public Object _remove(GlobalTransaction tx, String fqn, Object key, boolean create_undo_ops)
           throws CacheException
   {
      return _remove(tx, Fqn.fromString(fqn), key, create_undo_ops);
   }

   /**
    * Internal method to remove a key.
    *
    * @param fqn
    * @param key
    * @return Object
    */
   public Object _remove(GlobalTransaction tx, Fqn fqn, Object key, boolean create_undo_ops)
           throws CacheException
   {
      DataNode n = null;
      MethodCall undo_op = null;
      Object old_value = null;

      if (log.isTraceEnabled())
         log.trace(new StringBuffer("_remove(").append(tx).append(", \"").append(fqn).append("\", ").append(key).append(")"));

      // Find the node. This will lock it (if <tt>locking</tt> is true) and
      // add the temporarily created parent nodes to the TX's node list if tx != null)
      n = findNode(fqn);
      if (n == null)
      {
         log.warn("node " + fqn + " not found");
         return null;
      }
      notifyNodeModify(fqn, true);
      old_value = n.remove(key);

      // create a compensating method call (reverting the effect of
      // this modification) and put it into the TX's undo list.
      if (tx != null && create_undo_ops && old_value != null)
      {
         undo_op = MethodCallFactory.create(MethodDeclarations.putKeyValMethodLocal,
                 new Object[]{tx, fqn, key, old_value,
                         Boolean.FALSE});
         // 1. put undo-op in TX' undo-operations list (needed to rollback TX)
         tx_table.addUndoOperation(tx, undo_op);
      }

      notifyNodeModified(fqn); // changed from notifyNodeRemoved() - Jimmy Wilson
      notifyNodeModify(fqn, false);
      return old_value;
   }

   /**
    * Internal method to remove data from a node.
    */
   public void _removeData(GlobalTransaction tx, String fqn, boolean create_undo_ops)
           throws CacheException
   {
      _removeData(tx, Fqn.fromString(fqn), create_undo_ops);
   }

   /**
    * Internal method to remove data from a node.
    */
   public void _removeData(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops)
           throws CacheException
   {
      _removeData(tx, fqn, create_undo_ops, true);
   }

   /**
    * Internal method to remove data from a node.
    */
   public void _removeData(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, boolean sendNodeEvent)
           throws CacheException
   {
      _removeData(tx, fqn, create_undo_ops, sendNodeEvent, false);
   }

   /**
    * Internal method to remove data from a node.
    */
   public void _removeData(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, boolean sendNodeEvent, boolean eviction)
           throws CacheException
   {
      _removeData(tx, fqn, create_undo_ops, sendNodeEvent, eviction, null);
   }

   /**
    * Internal method to remove data from a node.
    */
   public void _removeData(GlobalTransaction tx, Fqn fqn, boolean create_undo_ops, boolean sendNodeEvent, boolean eviction, DataVersion version)
           throws CacheException
   {
      DataNode n = null;
      MethodCall undo_op = null;
      Map old_data = null;

      if (log.isTraceEnabled())
         log.trace(new StringBuffer("_removeData(").append(tx).append(", \"").append(fqn).append("\")"));

      // Find the node. This will lock it (if <tt>locking</tt> is true) and
      // add the temporarily created parent nodes to the TX's node list if tx != null)
      n = findNode(fqn, version);
      if (n == null)
      {
         log.warn("node " + fqn + " not found");
         return;
      }

      // create a compensating method call (reverting the effect of
      // this modification) and put it into the TX's undo list.
      if (tx != null && create_undo_ops && (old_data = n.getData()) != null && !eviction)
      {
         undo_op = MethodCallFactory.create(MethodDeclarations.putDataMethodLocal, new Object[]{tx, fqn, new HashMap(old_data), Boolean.FALSE});
      }

      if (eviction)
         notifyNodeEvict(fqn, true);
      else
         notifyNodeModify(fqn, true);

      n.clear();
      if (eviction)
         n.put(UNINITIALIZED, null); // required by cache loader to subsequently load the element again

      if (sendNodeEvent)
      {
         notifyNodeVisited(fqn);
      }
      else
      { // FIXME Bela did this so GUI view can refresh the view after node is evicted. But this breaks eviction policy, especially AOP!!!!
         if (eviction)
         {
            notifyNodeEvicted(fqn);
            notifyNodeEvict(fqn, false);
         }
         else
         {
            notifyNodeModified(fqn); // todo: merge these 2 notifications back into 1 !
            notifyNodeModify(fqn, false);
         }
      }

      // put undo-op in TX' undo-operations list (needed to rollback TX)
      if (tx != null && create_undo_ops)
      {
         tx_table.addUndoOperation(tx, undo_op);
      }
   }


   /**
    * Internal evict method called by eviction policy provider.
    *
    * @param fqn removes everything assoicated with this FQN
    * 
    * @return <code>true</code> if the node has been completely removed, 
    *         <code>false</code> if only the data map was removed, due
    *         to the presence of children
    *         
    * @throws CacheException
    */
   public boolean _evict(Fqn fqn) throws CacheException
   {
      if (!exists(fqn)) 
         return true;   // node does not exist. Maybe it has been recursively removed.
      // use remove method now if there is a child node. Otherwise, it is removed
      boolean create_undo_ops = false;
      boolean sendNodeEvent = false;
      boolean eviction = true;
      if (log.isTraceEnabled())
         log.trace("_evict(" + fqn + ")");
      if (hasChild(fqn))
      {
         _removeData(null, fqn, create_undo_ops, sendNodeEvent, eviction);
         return false;
      }
      else
      {
         _remove(null, fqn, create_undo_ops, sendNodeEvent, eviction);
         return true;
      }
   }

   /**
    * Internal evict method called by eviction policy provider.
    *
    * @param fqn
    * @param version
    * 
    * @return <code>true</code> if the node has been completely removed, 
    *         <code>false</code> if only the data map was removed, due
    *         to the presence of children
    *         
    * @throws CacheException
    */
   public boolean _evict(Fqn fqn, DataVersion version) throws CacheException
   {
      if (!exists(fqn)) 
         return true;  // node does not exist

      boolean create_undo_ops = false;
      boolean sendNodeEvent = false;
      boolean eviction = true;
      if (log.isTraceEnabled())
         log.trace("_evict(" + fqn + ", " + version + ")");
      if (hasChild(fqn))
      {
         _removeData(null, fqn, create_undo_ops, sendNodeEvent, eviction, version);
         return false;
      }
      else
      {
         _remove(null, fqn, create_undo_ops, sendNodeEvent, eviction, version);
         return true;
      }
   }

   /**
    * Evicts a key/value pair from a node's attributes. Note that this is <em>local</em>, will not be replicated.
    * @param fqn
    * @param key
    * @throws CacheException
    */
//    public void _evict(Fqn fqn, Object key) throws CacheException {
//       if(!exists(fqn)) return;
//       boolean create_undo_ops = false;
//       boolean sendNodeEvent = false;
//       boolean eviction=true;
//       _removeData(null, fqn, create_undo_ops, sendNodeEvent, eviction);
//    }


   /**
    * Compensating method to {@link #_remove(GlobalTransaction,Fqn,boolean)}.
    */
   public void _addChild(GlobalTransaction tx, Fqn parent_fqn, Object child_name, DataNode old_node)
           throws CacheException
   {
      if (log.isTraceEnabled())
         log.trace(new StringBuffer("_addChild(").append(tx).append(", \"").append(parent_fqn).
                 append("\", \"").append(child_name).append("\")"));

      if (parent_fqn == null || child_name == null || old_node == null)
      {
         log.error("parent_fqn or child_name or node was null");
         return;
      }
      DataNode tmp = findNode(parent_fqn);
      if (tmp == null)
      {
         log.warn("node " + parent_fqn + " not found");
         return;
      }
      tmp.addChild(child_name, old_node);
      // make sure any deleted markers are removed from this child.
      old_node.unmarkForRemoval(true);
      notifyNodeCreated(new Fqn(parent_fqn, child_name));
   }


   /**
    * Replicates changes across to other nodes in the cluster.  Invoked by the
    * ReplicationInterceptor.  Calls need to be forwarded to the
    * ReplicationInterceptor in this interceptor chain. This method will later
    * be moved entirely into the ReplicationInterceptor.
    */
   public Object _replicate(MethodCall method_call) throws Throwable
   {
      if (log.isTraceEnabled()) log.trace(getLocalAddress() + " received call " + method_call);
      JBCMethodCall jbcCall = (JBCMethodCall) method_call;
      try
      {
         getInvocationContext().setOriginLocal(false);

         Object result = invokeMethod(method_call);

         // Patch from Owen Taylor - JBCACHE-766
         // We replicating, we don't need to return the return value of the put-key-value
         // methods and the return values will cause marshalling problems, so we
         // omit them.
         if (jbcCall.getMethodId() == MethodDeclarations.putKeyValMethodLocal_id ||
                 jbcCall.getMethodId() == MethodDeclarations.putFailFastKeyValueMethodLocal_id ||
                 jbcCall.getMethodId() == MethodDeclarations.removeKeyMethodLocal_id)
            return null;
         else
            return result;
      }
      catch (Exception ex)
      {
         // patch from Owen Taylor (otaylor@redhat.com) to fix JBCACHE-786
         // The point of putFailFast() is to allow the caller to catch and ignore timeouts;
         // but they don't get a chance to do that for the (always async) replicated version,
         // so we just ignore timeout exceptions for putFailFast here.

         if (jbcCall.getMethodId() == MethodDeclarations.putFailFastKeyValueMethodLocal_id && ex instanceof TimeoutException)
         {
            log.debug("ignoring timeout exception when replicating putFailFast");
            return null;
         }

         log.warn("replication failure with method_call " + method_call + " exception", ex);
         throw ex;
      }
      finally
      {
         getInvocationContext().setOriginLocal(true);
      }
   }

   /**
    * Replicates a list of method calls.
    */
   public void _replicate(List method_calls) throws Throwable
   {
      Iterator it = method_calls.iterator();
      while (it.hasNext()) _replicate((MethodCall) it.next());
   }

   /**
    * A 'clustered get' call, called from a remote ClusteredCacheLoader.
    *
    * @return a List containing 2 elements: (Boolean.TRUE or Boolean.FALSE) and a value (Object).  If buddy replication
    *         is used one further element is added - an Fqn of the backup subtree in which this node may be found.
    */
   public List _clusteredGet(MethodCall methodCall, Boolean searchBackupSubtrees)
   {
      JBCMethodCall call = (JBCMethodCall) methodCall;
      if (log.isTraceEnabled()) log.trace("Clustered Get called with params: " + call + ", " + searchBackupSubtrees);
      Method m = call.getMethod();
      Object[] args = call.getArgs();

      Object callResults = null;

      try
      {
         Fqn fqn = (Fqn) args[0];

         if (log.isTraceEnabled()) log.trace("Clustered get: invoking call " + m + " with Fqn " + fqn);
         callResults = m.invoke(this, args);
         boolean found = validResult(callResults, call, fqn);
         if (log.isTraceEnabled()) log.trace("Got result " + callResults + ", found=" + found);
         if (found && callResults == null) callResults = createEmptyResults(call);
      }
      catch (Exception e)
      {
         log.warn("Problems processing clusteredGet call", e);
      }

      List results = new ArrayList(2);
      if (callResults != null)
      {
         results.add(Boolean.TRUE);
         results.add(callResults);
      }
      else
      {
         results.add(Boolean.FALSE);
         results.add(null);
      }
      return results;
   }

   /**
    * Used with buddy replication's data gravitation interceptor
    *
    * @param fqn            the fqn to gravitate
    * @param searchSubtrees should _BUDDY_BACKUP_ subtrees be searched
    * @param marshal        should the list of NodeData being gravitated be marshalled into
    *                       a byte[] or returned as a List
    * @return <code>List</code> with 1 or 3 elements. First element is a
    *         <code>Boolean</code> indicating whether data was found.  If
    *         <code>Boolean.FALSE</code>, the list will only have one element.
    *         Otherwise, second element is the data itself, structured as
    *         a <code>List</code> of <code>NodeData</code> objects, each of
    *         which represents one <code>Node</code> in the subtree rooted
    *         at <code>fqn</code>. If param <code>mnarshal</code> is
    *         <code>true</code>, this second element will have been marshalled
    *         to a <code>byte[]</code>, otherwise it will be the raw list.
    *         The third element represents the Fqn in the _BUDDY_BACKUP_
    *         region that needs to be cleaned in order to remove this data
    *         once the new owner has acquired it.
    */
   public List _gravitateData(Fqn fqn, boolean searchSubtrees, boolean marshal)
           throws CacheException
   {
      InvocationContext ctx = getInvocationContext();
      Option opt = ctx.getOptionOverrides();
      try
      {
         ctx.setOriginLocal(false);
         // we need to get the state for this Fqn and it's sub-nodes.

         // for now, perform a very simple series of getData calls.
         // use a get() call into the cache to make sure cache loading takes place.
         opt.setSkipDataGravitation(true);
         DataNode actualNode = get(fqn);
         // The call to get(Fqn) has changed the Option instance associated
         // with the thread. We need to restore the state of the original option,
         // and then restore it to the thread.  This is necessary because this
         // method can be invoked *directly* by DataGravitatorInterceptor; we need
         // to restore whatever options were in effect when DGI was called.
         opt.setSkipDataGravitation(false);
         ctx.setOptionOverrides(opt);


         Fqn backupNodeFqn = null;
         if (actualNode == null && searchSubtrees)
         {
            DataNode backupSubtree = findNode(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN);
            if (backupSubtree != null)
            {
               Map children = backupSubtree.getChildren();
               if (children != null)
               {
                  Iterator childNames = children.keySet().iterator();
                  while (childNames.hasNext() && actualNode == null)
                  {
                     backupNodeFqn = BuddyManager.getBackupFqn(childNames.next().toString(), fqn);
                     Option curOpt = ctx.getOptionOverrides();
                     curOpt.setSkipDataGravitation(true);
                     actualNode = get(backupNodeFqn);
                     curOpt.setSkipDataGravitation(false);
                     ctx.setOptionOverrides(opt);
                  }
               }
            }
         }

         ArrayList retval;
         if (actualNode == null)
         {
            // not found anything.
            retval = new ArrayList(1);
            retval.add(Boolean.FALSE);
         }
         else
         {
            // make sure we LOAD data for this node!!
            //getData(actualNode.getFqn());
            retval = new ArrayList(3);
            retval.add(Boolean.TRUE);

            List list = getNodeData(new LinkedList(), actualNode);
            if (marshal)
            {
               try
               {
                  ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
                  MarshalledValueOutputStream maos = new MarshalledValueOutputStream(baos);
                  maos.writeObject(list);
                  maos.close();
                  retval.add(baos.toByteArray());
               }
               catch (IOException e)
               {
                  throw new CacheException("Failure marshalling subtree at " + fqn, e);
               }
            }
            else
            {
               retval.add(list);
            }

            if (backupNodeFqn == null)
            {
               backupNodeFqn = BuddyManager.getBackupFqn(BuddyManager.getGroupNameFromAddress(getLocalAddress()), fqn);
            }
            retval.add(backupNodeFqn);
         }
         return retval;
      }
      finally
      {
         ctx.setOriginLocal(true);
         // Make sure we restore the state of the original option,
         // and then restore it to the thread.
         opt.setSkipDataGravitation(false);
         ctx.setOptionOverrides(opt);
      }
   }

   private List getNodeData(List list, DataNode node)
   {
      NodeData data = new NodeData(BuddyManager.getActualFqn(node.getFqn()), node.getData());
      list.add(data);
      Map children = node.getChildren();
      if (children != null)
      {
         Iterator i = children.keySet().iterator();
         while (i.hasNext())
         {
            Object childName = i.next();
            DataNode childNode = (DataNode) children.get(childName);
            getNodeData(list, childNode);
         }
      }
      return list;
   }
   // ------------- start: buddy replication specific 'lifecycle' method calls

   public void _remoteAssignToBuddyGroup(BuddyGroup group, Map state) throws Exception
   {
      try
      {
         // these are remote calls and as such, should have their origins marked as remote.
         getInvocationContext().setOriginLocal(false);
         if (buddyManager != null) buddyManager.handleAssignToBuddyGroup(group, state);
      }
      finally
      {
         getInvocationContext().setOriginLocal(true);
      }
   }

   public void _remoteRemoveFromBuddyGroup(String groupName) throws BuddyNotInitException
   {
      try
      {
         // these are remote calls and as such, should have their origins marked as remote.
         getInvocationContext().setOriginLocal(false);
         if (buddyManager != null) buddyManager.handleRemoveFromBuddyGroup(groupName);
      }
      finally
      {
         getInvocationContext().setOriginLocal(true);
      }
   }

   public void _remoteAnnounceBuddyPoolName(IpAddress address, String buddyPoolName)
   {
      try
      {
         // these are remote calls and as such, should have their origins marked as remote.
         getInvocationContext().setOriginLocal(false);
         if (buddyManager != null) buddyManager.handlePoolNameBroadcast(address, buddyPoolName);
      }
      finally
      {
         getInvocationContext().setOriginLocal(true);
      }
   }

   public void _dataGravitationCleanup(GlobalTransaction gtx, Fqn primary, Fqn backup) throws Exception
   {
      MethodCall primaryDataCleanup, backupDataCleanup;
      if (buddyManager.isDataGravitationRemoveOnFind())
      {
         if (log.isTraceEnabled()) log.trace("DataGravitationCleanup: Removing primary (" + primary + ") and backup (" + backup + ")");
         primaryDataCleanup = MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal, new Object[]{null, primary, Boolean.FALSE});
         backupDataCleanup = MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal, new Object[]{null, backup, Boolean.FALSE});
      }
      else
      {
         if (log.isTraceEnabled()) log.trace("DataGravitationCleanup: Evicting primary (" + primary + ") and backup (" + backup + ")");
         primaryDataCleanup = MethodCallFactory.create(MethodDeclarations.evictNodeMethodLocal, new Object[]{primary});
         backupDataCleanup = MethodCallFactory.create(MethodDeclarations.evictNodeMethodLocal, new Object[]{backup});
      }

      invokeMethod(primaryDataCleanup);
      invokeMethod(backupDataCleanup);
   }

   // ------------- end: buddy replication specific 'lifecycle' method calls


   /**
    * Returns true if the call results returned a valid result.
    */
   private boolean validResult(Object callResults, JBCMethodCall mc, Fqn fqn)
   {
      switch (mc.getMethodId())
      {
         case MethodDeclarations.getDataMapMethodLocal_id:
         case MethodDeclarations.getChildrenNamesMethodLocal_id:
            return callResults != null || exists(fqn);
         case MethodDeclarations.existsMethod_id:
            return ((Boolean) callResults).booleanValue();
         default:
            return false;
      }
   }

   /**
    * Creates an empty Collection class based on the return type of the method called.
    */
   private Object createEmptyResults(JBCMethodCall mc)
   {
      switch (mc.getMethodId())
      {
         case MethodDeclarations.getDataMapMethodLocal_id:
            return new HashMap(0);
         case MethodDeclarations.getChildrenNamesMethodLocal_id:
            return new HashSet(0);
         default:
            return null;
      }
   }

   /**
    * Releases all locks for a FQN.
    */
   public void _releaseAllLocks(Fqn fqn)
   {
      DataNode n;

      try
      {
         n = findNode(fqn);
         if (n == null)
         {
            log.error("releaseAllLocks(): node " + fqn + " not found");
            return;
         }
         n.releaseAllForce();
      }
      catch (Throwable t)
      {
         log.error("releaseAllLocks(): failed", t);
      }
   }

   /**
    * Finds and returns the {@link org.jboss.cache.DataNode#toString()} value for the Fqn.
    * Returns null if not found or upon error.
    */
   public String _print(Fqn fqn)
   {
      try
      {
         DataNode n = findNode(fqn);
         if (n == null) return null;
         return n.toString();
      }
      catch (Throwable t)
      {
         return null;
      }
   }

   /**
    * Should not be called.
    */
   public void _lock(Fqn fqn, int lock_type, boolean recursive)
           throws TimeoutException, LockingException
   {
      log.warn("method _lock() should not be invoked on TreeCache");
   }

   // todo: these methods can be removed once we move 2PC entirely into {Replication/Lock}Interceptor

   /**
    * Throws UnsupportedOperationException.
    */
   public void optimisticPrepare(GlobalTransaction gtx, List modifications, Map data, Address address, boolean onePhaseCommit)
   {
      throw new UnsupportedOperationException("optimisticPrepare() should not be called on TreeCache directly");
   }

   /**
    * Throws UnsupportedOperationException.
    */
   public void prepare(GlobalTransaction global_tx, List modifications, Address coord, boolean onePhaseCommit)
   {
      throw new UnsupportedOperationException("prepare() should not be called on TreeCache directly");
   }

   /**
    * Throws UnsupportedOperationException.
    */
   public void commit(GlobalTransaction tx)//, Boolean hasMods)
   {
      throw new UnsupportedOperationException("commit() should not be called on TreeCache directly");
   }

   /**
    * Throws UnsupportedOperationException.
    */
   public void rollback(GlobalTransaction tx)//, Boolean hasMods)
   {
      throw new UnsupportedOperationException("rollback() should not be called on TreeCache directly");
   }

   /* ----------------- End of  Callbacks ---------------------- */

   /**
    * Adds an undo operatoin to the transaction table.
    */
   public void addUndoOperation(GlobalTransaction gtx, MethodCall undo_op)
   {
      tx_table.addUndoOperation(gtx, undo_op);
   }

   /**
    * Returns the CacheLoaderManager.
    */
   public CacheLoaderManager getCacheLoaderManager()
   {
      return cacheLoaderManager;
   }

   /**
    * Sets the CacheLoaderManager.
    */
   public void setCacheLoaderManager(CacheLoaderManager cacheLoaderManager)
   {
      this.cacheLoaderManager = cacheLoaderManager;
   }

   /*-------------------- MessageListener ----------------------*/

   class MessageListenerAdaptor implements MessageListener
   {
      final Log my_log;   // Need this to run under jdk1.3
      final boolean trace;

      MessageListenerAdaptor(Log log)
      {
         this.my_log = log;
         this.trace = my_log.isTraceEnabled();
      }

      /**
       * Callback, does nothing.
       */
      public void receive(Message msg)
      {
         if (trace)
            my_log.trace("Received message " + msg);
      }

      /**
       * Returns a copy of the current cache (tree). It actually returns a 2
       * element array of byte[], element 0 being the transient state (or null)
       * and element 1 being the persistent state (or null)
       */
      public byte[] getState()
      {
         try
         {
//            // We use the lock acquisition timeout rather than the
//            // state transfer timeout, otherwise we'd never try
//            // to break locks before the requesting node gives up
//            return cache._getState(Fqn.fromString(SEPARATOR),
//               cache.getLockAcquisitionTimeout(),
//               true,
//               true);
            // Until flush is in place, use the old mechanism
            // where we wait the full state retrieval timeout
            return _getState(Fqn.ROOT, getInitialStateRetrievalTimeout(), true, true);
         }
         catch (Throwable t)
         {
            // This shouldn't happen as we set "suppressErrors" to true,
            // but we have to catch the Throwable declared in the method sig
            my_log.error("Caught " + t.getClass().getName() +
                    " while responding to initial state transfer request;" +
                    " returning null");
            return null;
         }
      }

      public void setState(byte[] new_state)
      {
         try
         {

            if (new_state == null)
            {
               if (my_log.isDebugEnabled()) my_log.debug("transferred state is null (may be first member in cluster)");
            }
            else
               TreeCache.this._setState(new_state, root, null);

            isStateSet = true;
         }
         catch (Throwable t)
         {
            my_log.error("failed setting state", t);
            if (t instanceof Exception)
               setStateException = (Exception) t;
            else
               setStateException = new Exception(t);
         }
         finally
         {
            synchronized (stateLock)
            {
               // Notify wait that state has been set.
               stateLock.notifyAll();
            }
         }
      }

   }

   /*-------------------- End of MessageListener ----------------------*/

   /*----------------------- MembershipListener ------------------------*/

   public void viewAccepted(View new_view)
   {
      Vector new_mbrs = new_view.getMembers();

      // todo: if MergeView, fetch and reconcile state from coordinator
      // actually maybe this is best left up to the application ? we just notify them and let
      // the appl handle it ?

      log.info("viewAccepted(): " + new_view);
      synchronized (members)
      {
         boolean needNotification = false;
         if (new_mbrs != null)
         {
            // Determine what members have been removed
            // and roll back any tx and break any locks
            Vector removed = (Vector) members.clone();
            removed.removeAll(new_mbrs);
            removeLocksForDeadMembers(root, removed);

            members.removeAllElements();
            members.addAll(new_view.getMembers());

            needNotification = true;
         }

         // Now that we have a view, figure out if we are the coordinator
         coordinator = (members.size() != 0 && members.get(0).equals(getLocalAddress()));

         // now notify listeners - *after* updating the coordinator. - JBCACHE-662 
         if (needNotification) notifyViewChange(new_view);

         // Wake up any threads that are waiting to know who the members
         // are so they can figure out who the coordinator is
         members.notifyAll();
      }
   }


   /**
    * Called when a member is suspected.
    */
   public void suspect(Address suspected_mbr)
   {
   }

   /**
    * Blocks sending and receiving of messages until viewAccepted() is called.
    */
   public void block()
   {
   }

   /*------------------- End of MembershipListener ----------------------*/

   /* ------------------------------ Private methods --------------------------- */

   /**
    * Returns the transaction associated with the current thread. We get the
    * initial context and a reference to the TransactionManager to get the
    * transaction. This method is used by {@link #getCurrentTransaction()}
    */
   protected Transaction getLocalTransaction()
   {
      if (tm == null)
      {
         return null;
      }
      try
      {
         return tm.getTransaction();
      }
      catch (Throwable t)
      {
         return null;
      }
   }


   /**
    * Returns true if transaction is ACTIVE or PREPARING, false otherwise.
    */
   boolean isValid(Transaction tx)
   {
      if (tx == null) return false;
      int status = -1;
      try
      {
         status = tx.getStatus();
         return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
      }
      catch (SystemException e)
      {
         log.error("failed getting transaction status", e);
         return false;
      }
   }


   /**
    * Returns the transaction associated with the current thread.
    * If a local transaction exists, but doesn't yet have a mapping to a
    * GlobalTransaction, a new GlobalTransaction will be created and mapped to
    * the local transaction.  Note that if a local transaction exists, but is
    * not ACTIVE or PREPARING, null is returned.
    *
    * @return A GlobalTransaction, or null if no (local) transaction was associated with the current thread
    */
   public GlobalTransaction getCurrentTransaction()
   {
      return getCurrentTransaction(true);
   }

   /**
    * Returns the transaction associated with the thread; optionally creating
    * it if is does not exist.
    */
   public GlobalTransaction getCurrentTransaction(boolean createIfNotExists)
   {
      Transaction tx;

      if ((tx = getLocalTransaction()) == null)
      { // no transaction is associated with the current thread
         return null;
      }

      if (!isValid(tx))
      { // we got a non-null transaction, but it is not active anymore
         int status = -1;
         try
         {
            status = tx.getStatus();
         }
         catch (SystemException e)
         {
         }
         
         // JBCACHE-982 -- don't complain if COMMITTED
         if (status != Status.STATUS_COMMITTED)
         {
            log.warn("status is " + status + " (not ACTIVE or PREPARING); returning null)", new Throwable());
         }
         else
         {
            log.trace("status is COMMITTED; returning null");
         }
         
         return null;
      }

      return getCurrentTransaction(tx, createIfNotExists);
   }

   /**
    * Returns the global transaction for this local transaction.
    */
   public GlobalTransaction getCurrentTransaction(Transaction tx)
   {
      return getCurrentTransaction(tx, true);
   }

   /**
    * Returns the global transaction for this local transaction.
    *
    * @param createIfNotExists if true, if a global transaction is not found; one is created
    */
   public GlobalTransaction getCurrentTransaction(Transaction tx, boolean createIfNotExists)
   {
      // removed synchronization on tx_table because underlying implementation is thread safe
      // and JTA spec (section 3.4.3 Thread of Control, par 2) says that only one thread may
      // operate on the transaction at one time so no concern about 2 threads trying to call
      // this method for the same Transaction instance at the same time
      //
      GlobalTransaction gtx = tx_table.get(tx);
      if (gtx == null && createIfNotExists)
      {
         Address addr = (Address) getLocalAddress();
         gtx = GlobalTransaction.create(addr);
         tx_table.put(tx, gtx);
         TransactionEntry ent = isNodeLockingOptimistic() ? new OptimisticTransactionEntry() : new TransactionEntry();
         ent.setTransaction(tx);
         tx_table.put(gtx, ent);
         if (log.isTraceEnabled())
            log.trace("created new GTX: " + gtx + ", local TX=" + tx);
      }
      return gtx;
   }


   /**
    * Invokes a method against this object. Contains the logger_ic for handling
    * the various use cases, e.g. mode (local, repl_async, repl_sync),
    * transaction (yes or no) and locking (yes or no).
    */
   protected Object invokeMethod(MethodCall m) throws CacheException
   {
      try
      {
         return interceptor_chain.invoke(m);
      }
      catch (CacheException ce)
      {
         throw ce;
      }
      catch (RuntimeException re)
      {
         throw re;
      }
      catch (Throwable t)
      {
         throw new RuntimeException(t);
      }
   }

   /**
    * Returns an object suitable for use in node locking, either the current
    * transaction or the current thread if there is no transaction.
    */
   protected Object getOwnerForLock()
   {
      Object owner = getCurrentTransaction();
      if (owner == null)
         owner = Thread.currentThread();

      return owner;
   }

   /**
    * Loads the specified class using this class's classloader, or, if it is <code>null</code>
    * (i.e. this class was loaded by the bootstrap classloader), the system classloader.
    * <p/>
    * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this
    * class may be loaded by the bootstrap classloader.
    * </p>
    *
    * @throws ClassNotFoundException
    */
   protected Class loadClass(String classname) throws ClassNotFoundException
   {
      ClassLoader cl = getClass().getClassLoader();
      if (cl == null)
         cl = ClassLoader.getSystemClassLoader();
      return cl.loadClass(classname);
   }

   /**
    * Finds a node given a fully qualified name.
    * Whenever nodes are created, and the global transaction is not null, the created
    * nodes have to be added to the transaction's {@link TransactionEntry}
    * field.<br>
    * When a lock is acquired on a node, a reference to the lock has to be
    * {@link TransactionEntry#addLock(IdentityLock) added to the list of locked nodes}
    * in the {@link TransactionEntry}.
    * <p>This operation will also apply different locking to the tree nodes, depending on
    * <tt>operation_type</tt>. If it is <tt>read</tt> type, all nodes will be acquired with
    * read lock. Otherwise, the operation is <tt>write</tt> type, all parent nodes will be acquired
    * with read lock while the destination node acquires write lock.</p>
    *
    * @param fqn Fully qualified name for the corresponding node.
    * @return DataNode
    */
   private Node findNode(Fqn fqn)
   {
      try
      {
         return findNode(fqn, null);
      }
      catch (CacheException e)
      {
         log.warn("Unexpected error", e);
         return null;
      }
   }

   /**
    * Finds a node given a fully qualified name and DataVersion.
    */
   private Node findNode(Fqn fqn, DataVersion version) throws CacheException
   {
      if (fqn == null) return null;

      Node toReturn = findInternal(fqn, false);

      if (version != null)
      {
         // we need to check the version of the data node...
         DataVersion nodeVersion = ((OptimisticTreeNode) toReturn).getVersion();
         if (log.isDebugEnabled())
            log.debug("looking for optimistic node [" + fqn + "] with version [" + version + "].  My version is [" + nodeVersion + "]");
         if (nodeVersion.newerThan(version))
         {
            // we have a versioning problem; throw an exception!
            throw new CacheException("Unable to validate versions.");
         }

      }
      return toReturn;
   }

   /**
    * Returns the region manager for this TreeCache.
    */
   public RegionManager getRegionManager()
   {
      if (regionManager_ == null)
         regionManager_ = new RegionManager();
      return regionManager_;
   }

   /**
    * Returns the eviction region manager for this TreeCache.
    */
   public org.jboss.cache.eviction.RegionManager getEvictionRegionManager()
   {
      return evictionRegionManager_;
   }

   public VersionAwareMarshaller getMarshaller()
   {
      if (marshaller_ == null)
      {
         marshaller_ = new VersionAwareMarshaller(getRegionManager(), inactiveOnStartup, useRegionBasedMarshalling, getReplicationVersion());
      }
      return marshaller_;
   }

   /**
    * Sends a notification that a node was created.
    */
   public void notifyNodeCreated(Fqn fqn)
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.nodeCreated(fqn);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).nodeCreated(fqn);
      }
   }

   /**
    * Sends a notification that a node was loaded.
    */
   public void notifyNodeLoaded(Fqn fqn)
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.nodeLoaded(fqn);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).nodeLoaded(fqn);
      }
   }

   /**
    * Sends a notification that a node was activated.
    */
   public void notifyNodeActivate(Fqn fqn, boolean pre)
   {
      if (evictionPolicyListener != null)
      {
         if (evictionPolicyListener instanceof ExtendedTreeCacheListener)
         {
            ((ExtendedTreeCacheListener) evictionPolicyListener).nodeActivate(fqn, pre);
         }
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
         {
            Object listener = it.next();
            if (listener instanceof ExtendedTreeCacheListener)
            {
               ((ExtendedTreeCacheListener) listener).nodeActivate(fqn, pre);
            }
         }
      }
   }

   /**
    * Sends a notification that a node was passivated.
    */
   public void notifyNodePassivate(Fqn fqn, boolean pre)
   {
      if (evictionPolicyListener != null)
      {
         if (evictionPolicyListener instanceof ExtendedTreeCacheListener)
         {
            ((ExtendedTreeCacheListener) evictionPolicyListener).nodePassivate(fqn, pre);
         }
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
         {
            Object listener = it.next();
            if (listener instanceof ExtendedTreeCacheListener)
            {
               ((ExtendedTreeCacheListener) listener).nodePassivate(fqn, pre);
            }
         }
      }
   }

   public void notifyNodeRemove(Fqn fqn, boolean pre)
   {
      if (evictionPolicyListener != null)
      {
         if (evictionPolicyListener instanceof ExtendedTreeCacheListener)
         {
            ((ExtendedTreeCacheListener) evictionPolicyListener).nodeRemove(fqn, pre, getInvocationContext().isOriginLocal());
         }
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
         {
            Object listener = it.next();
            if (listener instanceof ExtendedTreeCacheListener)
            {
               ((ExtendedTreeCacheListener) listener).nodeRemove(fqn, pre, getInvocationContext().isOriginLocal());
            }
         }
      }
   }

   public void notifyNodeRemoved(Fqn fqn)
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.nodeRemoved(fqn);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).nodeRemoved(fqn);
      }
   }

   public void notifyNodeEvict(Fqn fqn, boolean pre)
   {
      if (evictionPolicyListener != null)
      {
         if (evictionPolicyListener instanceof ExtendedTreeCacheListener)
         {
            ((ExtendedTreeCacheListener) evictionPolicyListener).nodeEvict(fqn, pre);
         }
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
         {
            Object listener = it.next();
            if (listener instanceof ExtendedTreeCacheListener)
            {
               ((ExtendedTreeCacheListener) listener).nodeEvict(fqn, pre);
            }
         }
      }
   }

   public void notifyNodeEvicted(Fqn fqn)
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.nodeEvicted(fqn);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).nodeEvicted(fqn);
      }
   }


   public void notifyNodeModify(Fqn fqn, boolean pre)
   {
      if (evictionPolicyListener != null)
      {
         if (evictionPolicyListener instanceof ExtendedTreeCacheListener)
         {
            ((ExtendedTreeCacheListener) evictionPolicyListener).nodeModify(fqn, pre, getInvocationContext().isOriginLocal());
         }
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
         {
            Object listener = it.next();
            if (listener instanceof ExtendedTreeCacheListener)
            {
               ((ExtendedTreeCacheListener) listener).nodeModify(fqn, pre, getInvocationContext().isOriginLocal());
            }
         }
      }
   }

   public void notifyNodeModified(Fqn fqn)
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.nodeModified(fqn);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).nodeModified(fqn);
      }

   }

   public void notifyNodeVisited(Fqn fqn)
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.nodeVisited(fqn);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).nodeVisited(fqn);
      }
   }

   protected void notifyCacheStarted()
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.cacheStarted(this);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).cacheStarted(this);
      }
   }

   protected void notifyCacheStopped()
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.cacheStopped(this);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).cacheStopped(this);
      }
   }

   protected void notifyViewChange(View v)
   {
      if (evictionPolicyListener != null)
      {
         evictionPolicyListener.viewChange(v);
      }
      if (hasListeners)
      {
         for (Iterator it = listeners.iterator(); it.hasNext();)
            ((TreeCacheListener) it.next()).viewChange(v);
      }
   }

   /**
    * Generates NodeAdded notifications for all nodes of the tree. This is
    * called whenever the tree is initially retrieved (state transfer)
    */
   protected void notifyAllNodesCreated(DataNode curr)
   {
      DataNode n;
      Map children;

      if (curr == null) return;
      notifyNodeCreated(curr.getFqn());
      
      if ((children = curr.getChildren()) != null)
      {
         for (Iterator it = children.values().iterator(); it.hasNext();)
         {
            n = (DataNode) it.next();
            notifyAllNodesCreated(n);
         }
      }
   }

   /**
    * Returns the default JGroup properties.
    * Subclasses may wish to override this method.
    */
   protected String getDefaultProperties()
   {
      return "UDP(mcast_addr=224.0.0.36;mcast_port=55566;ip_ttl=32;" +
              "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
              "PING(timeout=1000;num_initial_members=2):" +
              "MERGE2(min_interval=5000;max_interval=10000):" +
              "FD_SOCK:" +
              "VERIFY_SUSPECT(timeout=1500):" +
              "pbcast.NAKACK(gc_lag=50;max_xmit_size=8192;retransmit_timeout=600,1200,2400,4800):" +
              "UNICAST(timeout=600,1200,2400,4800):" +
              "pbcast.STABLE(desired_avg_gossip=20000):" +
              "FRAG(frag_size=8192;down_thread=false;up_thread=false):" +
              "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
              "shun=false;print_local_addr=true):" +
              "pbcast.STATE_TRANSFER";
   }

   /**
    * Converts a replication, such as <code>repl-async</code> mode to an
    * integer.
    */
   protected int string2Mode(String mode)
   {
      if (mode == null) return -1;
      String m = mode.toLowerCase().trim().replace('_', '-');
      if (m.equals("local"))
         return LOCAL;
      else if (m.equals("repl-async"))
         return REPL_ASYNC;
      else if (m.equals("repl-sync"))
         return REPL_SYNC;
      else if (m.equals("invalidation-async"))
         return INVALIDATION_ASYNC;
      else if (m.equals("invalidation-sync"))
         return INVALIDATION_SYNC;

      return -1;
   }

   private void initialiseCacheLoaderManager() throws Exception
   {
      if (cacheLoaderManager == null)
      {
         cacheLoaderManager = new CacheLoaderManager();
      }
      if (cacheLoaderConfig != null)
      {
         // use the newer XML based config.
         cacheLoaderManager.setConfig(cacheLoaderConfig, this);
      }
      else
      {
         // use a legacy config as someone has used one of the legacy methods to set this.
         cacheLoaderManager.setConfig(cloaderConfig, this);
      }
   }

   /**
    * Sets the thread-local invocation context
    *
    * @param invocationContext the context for the current invocation
    */
   public void setInvocationContext(InvocationContext invocationContext)
   {
      invocationContextContainer.set(invocationContext);
   }

   /**
    * Retrieves the thread-local InvocationContext.
    *
    * @return the context for the current invocation
    */
   public InvocationContext getInvocationContext()
   {
      InvocationContext ctx = (InvocationContext) invocationContextContainer.get();
      if (ctx == null)
      {
         ctx = new InvocationContext();
         invocationContextContainer.set(ctx);
      }
      return ctx;
   }

   // ---------------------------------------------------------------
   // START: Methods to provide backward compatibility with older cache loader config settings
   // ---------------------------------------------------------------

   /**
    * Sets the cache loader class name.
    *
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderClass(String cache_loader_class)
   {
      log.warn("Using deprecated config element CacheLoaderClass.  This element will be removed in future, please use CacheLoaderConfiguration instead.");
      initDeprecatedCacheLoaderConfig();
      cloaderConfig.getFirstCacheLoaderConfig().setClassName(cache_loader_class);
   }

   /**
    * Sets the cache loader configuration.
    *
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderConfig(Properties cache_loader_config)
   {
      log.warn("Using deprecated config element CacheLoaderConfig(Properties).  This element will be removed in future, please use CacheLoaderConfiguration instead.");
      initDeprecatedCacheLoaderConfig();
      cloaderConfig.getFirstCacheLoaderConfig().setProperties(cache_loader_config);
   }

   /**
    * Sets the cache loader shared state.
    *
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderShared(boolean shared)
   {
      log.warn("Using deprecated config element CacheLoaderShared.  This element will be removed in future, please use CacheLoaderConfiguration instead.");
      initDeprecatedCacheLoaderConfig();
      cloaderConfig.setShared(shared);
   }

   /**
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderPassivation(boolean passivate)
   {
      log.warn("Using deprecated config element CacheLoaderPassivation.  This element will be removed in future, please use CacheLoaderConfiguration instead.");
      initDeprecatedCacheLoaderConfig();
      cloaderConfig.setPassivation(passivate);
   }

   /**
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderPreload(String list)
   {
      log.warn("Using deprecated config element CacheLoaderPreload.  This element will be removed in future, please use CacheLoaderConfiguration instead.");
      initDeprecatedCacheLoaderConfig();
      cloaderConfig.setPreload(list);
   }

   /**
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderAsynchronous(boolean b)
   {
      log.warn("Using deprecated config element CacheLoaderAsynchronous.  This element will be removed in future, please use CacheLoaderConfiguration instead.");
      initDeprecatedCacheLoaderConfig();
      cloaderConfig.getFirstCacheLoaderConfig().setAsync(b);
   }

   /**
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderFetchPersistentState(boolean flag)
   {
      log.warn("Using deprecated config element CacheLoaderFetchPersistentState.  This element will be removed in future, please use CacheLoaderConfiguration instead.");
      initDeprecatedCacheLoaderConfig();
      cloaderConfig.getFirstCacheLoaderConfig().setFetchPersistentState(flag);
   }

   /**
    * @see #setCacheLoaderConfiguration(org.w3c.dom.Element)
    * @deprecated
    */
   public void setCacheLoaderFetchTransientState(boolean flag)
   {
      log.warn("Using deprecated config element CacheLoaderFetchTransientState.  This element will be removed in future, replaced with FetchInMemoryState.");
      setFetchInMemoryState(flag);
   }

   private void initDeprecatedCacheLoaderConfig()
   {
      // legacy config will only use a single cache loader.
      if (cloaderConfig == null)
      {
         cloaderConfig = new CacheLoaderConfig();
         log.warn("Using legacy cache loader config mechanisms.");
         if (cacheLoaderConfig != null) log.warn("Specified CacheLoaderConfiguration XML block will be ignored!");
      }
      if (cloaderConfig.getIndividualCacheLoaderConfigs().size() == 0)
      {
         CacheLoaderConfig.IndividualCacheLoaderConfig first = new CacheLoaderConfig.IndividualCacheLoaderConfig();
         cloaderConfig.addIndividualCacheLoaderConfig(first);
      }
   }

   /**
    * Sets the CacheLoader to use.
    * Provided for backwards compatibility.
    *
    * @param loader
    * @deprecated
    */
   public void setCacheLoader(CacheLoader loader)
   {
      log.warn("Using deprecated config method setCacheLoader.  This element will be removed in future, please use CacheLoaderConfiguration instead.");

      try
      {
         if (cacheLoaderManager == null) initialiseCacheLoaderManager();
      }
      catch (Exception e)
      {
         log.warn("Problem setting cache loader.  Perhaps your cache loader config has not been set yet?");
      }
      cacheLoaderManager.setCacheLoader(loader);
   }

   /**
    * Returns the cache loader class name.
    * Provided for backward compatibility.  Use {@link #getCacheLoaderConfiguration} instead.
    *
    * @deprecated
    */
   public String getCacheLoaderClass()
   {
      return cacheLoaderManager == null ? null : cacheLoaderManager.getCacheLoaderConfig().getFirstCacheLoaderConfig().getClassName();
   }

   /**
    * Returns false always.
    * Provided for backward compatibility.  Use {@link #getCacheLoaderConfiguration} instead.
    *
    * @deprecated
    */
   public boolean getCacheLoaderShared()
   {
      return false;
   }

   /**
    * Returns true if passivation is on.
    * Provided for backward compatibility.  Use {@link #getCacheLoaderConfiguration} instead.
    *
    * @deprecated
    */
   public boolean getCacheLoaderPassivation()
   {
      return cacheLoaderManager != null && cacheLoaderManager.getCacheLoaderConfig().isPassivation();
   }

   /**
    * Returns true if the cache loader is asynchronous.
    * Provided for backward compatibility.  Use {@link #getCacheLoaderConfiguration} instead.
    *
    * @deprecated
    */
   public boolean getCacheLoaderAsynchronous()
   {
      return cacheLoaderManager != null && cacheLoaderManager.getCacheLoaderConfig().getFirstCacheLoaderConfig().isAsync();
   }

   /**
    * Provided for backward compatibility.  Use {@link #getCacheLoaderConfiguration} instead.
    *
    * @deprecated
    */
   public String getCacheLoaderPreload()
   {
      return cacheLoaderManager == null ? null : cacheLoaderManager.getCacheLoaderConfig().getPreload();
   }

   /**
    * Provided for backward compatibility.  Use {@link #getCacheLoaderConfiguration} instead.
    *
    * @deprecated
    */
   public boolean getCacheLoaderFetchPersistentState()
   {
      return cacheLoaderManager != null && cacheLoaderManager.getCacheLoaderConfig().getFirstCacheLoaderConfig().isFetchPersistentState();
   }

   /**
    * Provided for backward compatibility.  Use {@link #getCacheLoaderConfiguration} instead.
    *
    * @deprecated
    */
   public boolean getCacheLoaderFetchTransientState()
   {
      return getFetchInMemoryState();
   }

   /**
    * Returns the properties in the cache loader configuration.
    */
   public Properties getCacheLoaderConfig()
   {
      if (cacheLoaderManager == null)
         return null;
      return cacheLoaderManager.getCacheLoaderConfig().getFirstCacheLoaderConfig().getProperties();
   }

   /**
    * Purges the contents of all configured {@link CacheLoader}s
    */
   public void purgeCacheLoaders() throws Exception
   {
      if (cacheLoaderManager != null) cacheLoaderManager.purgeLoaders(true);
   }

   // ---------------------------------------------------------------
   // END: Methods to provide backward compatibility with older cache loader config settings
   // ---------------------------------------------------------------

   private JChannel getMultiplexerChannel(String serviceName, String stackName)
   {
      if (serviceName == null || serviceName.length() == 0)
         return null;

      MBeanServer mbserver = getMBeanServer();
      if (mbserver == null)
      {
         log.warn("Multiplexer service specified but MBean server not found." +
                 "  Multiplexer will not be used for cache cluster " + cluster_name + ".");
         return null;
      }

      try
      {
         ObjectName muxName = new ObjectName(serviceName);

         // see if Multiplexer service is registered
         if (!mbserver.isRegistered(muxName))
         {
            log.warn("Multiplexer service specified but '" + serviceName + "' not registered." +
                    "  Multiplexer will not be used for cache cluster " + cluster_name + ".");
            return null;
         }

         // see if createMultiplexerChannel() is supported
         boolean muxFound = false;
         MBeanOperationInfo[] ops = mbserver.getMBeanInfo(muxName).getOperations();
         for (int i = 0; i < ops.length; i++)
         {
            MBeanOperationInfo op = ops[i];
            if (op.getName().equals(CREATE_MUX_CHANNEL))
            {
               muxFound = true;
               break;
            }
         }
         if (!muxFound)
         {
            log.warn("Multiplexer service registered but method '" + CREATE_MUX_CHANNEL + "' not found." +
                    "  Multiplexer will not be used for cache cluster " + cluster_name + "." +
                    "  Ensure that you are using JGroups version 2.3 or later.");
            return null;
         }

         // create the multiplexer channel and return as a JChannel instance
         Object[] params = {stackName, cluster_name};
         return (JChannel) mbserver.invoke(muxName, CREATE_MUX_CHANNEL, params, MUX_TYPES);
      }
      catch (Exception e)
      {
         log.error("Multiplexer channel creation failed." +
                 "  Multiplexer will not be used for cache cluster " + cluster_name + ".", e);
         return null;
      }
   }

   private MBeanServer getMBeanServer()
   {
      // return local server from ServiceMBeanSupport if available
      if (server != null)
         return server;

      ArrayList servers = MBeanServerFactory.findMBeanServer(null);
      if (servers == null || servers.size() == 0)
         return null;

      // return 'jboss' server if available
      for (int i = 0; i < servers.size(); i++)
      {
         MBeanServer server = (MBeanServer) servers.get(i);
         if (server != null && server.getDefaultDomain() != null && server.getDefaultDomain().equalsIgnoreCase(JBOSS_SERVER_DOMAIN))
            return server;
      }

      // return first available server
      return (MBeanServer) servers.get(0);

   }
   
   protected void registerChannelInJmx()
   {
      if (server != null)
      {
         try
         {
            String protocolPrefix = JGROUPS_JMX_DOMAIN + ":" + PROTOCOL_JMX_ATTRIBUTES + getClusterName();
            JmxConfigurator.registerProtocols(server, channel, protocolPrefix);
            protocolsRegistered = true;
            
            String name = JGROUPS_JMX_DOMAIN + ":" + CHANNEL_JMX_ATTRIBUTES + getClusterName();
            JmxConfigurator.registerChannel(channel, server, name);
            channelRegistered = true;
         }
         catch (Exception e)
         {
            log.error("Caught exception registering channel in JXM", e);
         }
      }
   }
   
   protected void unregisterChannelFromJmx()
   {
      ObjectName on = null;
      if (channelRegistered)
      {          
         // Unregister the channel itself
         try
         {
            on = new ObjectName(JGROUPS_JMX_DOMAIN + ":" + CHANNEL_JMX_ATTRIBUTES + getClusterName());
            server.unregisterMBean(on);
         }
         catch (Exception e)
         {
            if (on != null)
               log.error("Caught exception unregistering channel at " + on, e);
            else
               log.error("Caught exception unregistering channel", e);
         }
      }
      
      if (protocolsRegistered)
      {
         // Unregister the protocols
         try
         {
            on = new ObjectName(JGROUPS_JMX_DOMAIN + ":*," + PROTOCOL_JMX_ATTRIBUTES + getClusterName());
            Set mbeans=server.queryNames(on, null);
            if(mbeans != null) {
                for(Iterator it=mbeans.iterator(); it.hasNext();) {
                    server.unregisterMBean((ObjectName)it.next());
                }
            }
         }
         catch (Exception e)
         {
            if (on != null)
               log.error("Caught exception unregistering protocols at " + on, e);
            else
               log.error("Caught exception unregistering protocols", e);
         }
      }
   }

   static interface TransactionLockStatus extends Status
   {
      public static final int STATUS_BROKEN = Integer.MIN_VALUE;
   }


}
