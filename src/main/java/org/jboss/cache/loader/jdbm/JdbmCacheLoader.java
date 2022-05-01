package org.jboss.cache.loader.jdbm;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.TreeCache;
import org.jboss.cache.buddyreplication.BuddyManager;
import org.jboss.cache.loader.ExtendedCacheLoader;
import org.jboss.cache.marshall.RegionManager;
import org.jboss.cache.optimistic.FqnComparator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;


/**
 * A persistent <code>CacheLoader</code> based on the JDBM project.
 * See http://jdbm.sourceforge.net/ .
 * Does not support transaction isolation.
 *
 * <p>The configuration string format is:</p>
 * <pre>environmentDirectoryName[#databaseName]</pre>
 * <p>where databaseName, if omitted, defaults to the ClusterName property
 * of the TreeCache.</p>
 * <p/>
 * Data is sorted out like:
<pre>
/ = N
/node1 = N
/node1/K/k1 = v1
/node1/K/k2 = v2
/node2 = N
/node2/node3 = N
/node2/node3/K/k1 = v1
/node2/node3/K/k2 = v2
/node2/node4 = N
</pre>
 * N represents a node, K represents a key block. k and v represent key/value
 * pairs.
 * <p/>
 * TODO the browse operations lock the entire tree; eventually the JDBM team
 * plans to fix this.
 *
 * @author Elias Ross
 * @version $Id: JdbmCacheLoader.java 1959 2006-05-24 05:47:43Z bstansberry $
 */
public class JdbmCacheLoader implements ExtendedCacheLoader {

   private static final Log log = LogFactory.getLog(JdbmCacheLoader.class); 

   private static final String KEYS = "K";
   private static final String NODE = "N";
   private static final String NAME = "JdbmCacheLoader";

   private String locationStr;
   private TreeCache treeCache;
   private String cacheDbName;
   private RecordManager recman;
   private BTree tree;
   private Map transactions = new ConcurrentHashMap();
   private RegionManager manager;

   /*
    * Service implementation -- lifecycle methods.
    * Note that setConfig() and setCache() are called before create().
    */

   public void create() throws Exception {
      checkNotOpen();
   }

   public void destroy() {
   }

   /**
    * Opens the environment and the database specified by the configuration
    * string.  The environment and databases are created if necessary.
    */
   public void start()
      throws Exception {

      log.trace("Starting JdbmCacheLoader instance.");
      checkNotOpen();
      checkNonNull(treeCache, "TreeCache object is required");

      if (locationStr == null) {
         locationStr=System.getProperty("java.io.tmpdir");
      }

      // test location
      File location = new File(locationStr);
      if (!location.exists())
      {
          boolean created = location.mkdirs();
          if (!created) throw new IOException("Unable to create cache loader location " + location);

      }
      if (!location.isDirectory()) throw new IOException("Cache loader location [" + location + "] is not a directory!");
       

      /* Parse config string. */
      File homeDir;
      int offset = locationStr.indexOf('#');
      if (offset >= 0 && offset < locationStr.length() - 1) {
         homeDir = new File(locationStr.substring(0, offset));
         cacheDbName = locationStr.substring(offset + 1);
      } else {
         homeDir = new File(locationStr);
         cacheDbName = treeCache.getClusterName();
      }

      try {
         openDatabase(new File(homeDir, cacheDbName));
      } catch (Exception e) {
         destroy();
         throw e;
      }
   }

   /**
    * Opens all databases and initializes database related information.
    */
   private void openDatabase(File f)
      throws Exception
   {
      Properties props = new Properties();
      // Incorporate properties from setConfig() ?
      // props.put(RecordManagerOptions.SERIALIZER, RecordManagerOptions.SERIALIZER_EXTENSIBLE);
      // props.put(RecordManagerOptions.PROFILE_SERIALIZATION, "false");
      recman = RecordManagerFactory.createRecordManager(f.toString(), props);
      long recid = recman.getNamedObject(NAME);
      log.debug(NAME + " located as " + recid);
      if (recid == 0) {
      tree = BTree.createInstance(recman, new FqnComparator());
         recman.setNamedObject(NAME, tree.getRecid());
      } else {
         tree = BTree.load(recman, recid);
      }

      log.info("JDBM database " + f + " opened with " + tree.size() + " entries");
   }

   /**
    * Closes all databases, ignoring exceptions, and nulls references to all
    * database related information.
    */
   private void closeDatabases() {
      if (recman != null) {
         try {
            recman.close();
         } catch (Exception shouldNotOccur) {
            log.warn("Caught unexpected exception", shouldNotOccur);
         }
      }
      recman = null;
      tree = null;
   }

   /**
    * Closes the databases and environment, and nulls references to them.
    */
   public void stop() {
      log.debug("stop");
      closeDatabases();
   }

   /*
    * CacheLoader implementation.
    */

   /**
    * Sets the configuration string for this cache loader.
    */
   public void setConfig(Properties props) {
      checkNotOpen();
      locationStr = props != null? props.getProperty("location") : null;
      if (log.isTraceEnabled()) log.trace("Configuring cache loader with location = " + locationStr);
   }

   /**
    * Sets the TreeCache owner of this cache loader.
    */
   public void setCache(TreeCache c) {
      checkNotOpen();
      treeCache = c;
   }

   /**
    * Returns a special FQN for keys of a node.
    */
   private Fqn keys(Fqn name) {
      return new Fqn(name, KEYS);
   }

   /**
    * Returns a special FQN for key of a node.
    */
   private Fqn key(Fqn name, Object key) {
      return new Fqn(name, KEYS, nullMask(key));
   }

   /**
    * Returns an unmodifiable set of relative children names, or
    * returns null if the parent node is not found or if no children are found.
    * This is a fairly expensive operation, and is assumed to be performed by
    * browser applications.  Calling this method as part of a run-time
    * transaction is not recommended.
    */
   public Set getChildrenNames(Fqn name)
      throws Exception
   {

      if (log.isTraceEnabled())
         log.trace("getChildrenNames " + name);

      synchronized (tree) {
         return getChildrenNames0(name);
      }
   }

   private Set getChildrenNames0(Fqn name) throws IOException {
      TupleBrowser browser = tree.browse(name);
      Tuple t = new Tuple();

      if (browser.getNext(t)) {
         if (!t.getValue().equals(NODE)) {
            log.trace(" not a node");
            return null;
         }
      } else {
         log.trace(" no nodes");
         return null;
      }

      Set set = new HashSet();

      // Want only /a/b/c/X nodes
      int depth = name.size() + 1;
      while (browser.getNext(t)) {
         Fqn fqn = (Fqn)t.getKey();
         int size = fqn.size();
         if (size < depth)
            break;
         if (size == depth && t.getValue().equals(NODE))
            set.add(fqn.getLast());
      }

      if (set.isEmpty())
         return null;

      return Collections.unmodifiableSet(set);
   }

   /**
    * Returns a map containing all key-value pairs for the given FQN, or null
    * if the node is not present.
    * This operation is always non-transactional, even in a transactional
    * environment.
    */
   public Map get(Fqn name)
      throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      if (tree.find(name) == null) {
         if (log.isTraceEnabled())
            log.trace("get, no node: " + name);
         return null;
      }

      Fqn keys = keys(name);
      Tuple t = new Tuple();
      Map map = new HashMap();

      synchronized (tree) {
         TupleBrowser browser = tree.browse(keys);
         while (browser.getNext(t)) {
            Fqn fqn = (Fqn)t.getKey();
            if (!fqn.isChildOf(keys))
               break;
            Object k = fqn.getLast();
            Object v = t.getValue();
            map.put(nullUnmask(k), nullUnmask(v));
         }
      }

      if (log.isTraceEnabled())
         log.trace("get " + name + " map=" + map);

      return map;
   }

   /**
    * Returns whether the given node exists.
    */
   public boolean exists(Fqn name) throws IOException {
      return tree.find(name) != null;
   }

   private void commit() throws Exception {
      recman.commit();
   }

   /**
    * Stores a single FQN-key-value record.
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public Object put(Fqn name, Object key, Object value) throws Exception {
      try {
         return put0(name, key, value);
      } finally {
         commit();
      }
   }

   private Object put0(Fqn name, Object key, Object value) throws Exception {
      checkNonNull(name, "name");
      makeNode(name);
      Fqn rec = key(name, key);
      Object oldValue = insert(rec, value);
      if (log.isTraceEnabled())
         log.trace("put " + rec + " value=" + value + " old=" + oldValue);
      return oldValue;
   }

   /**
    * Stores a map of key-values for a given FQN, but does not delete existing
    * key-value pairs (that is, it does not erase).
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public void put(Fqn name, Map values) throws Exception {
      put0(name, values);
      commit();
   }

   private void put0(Fqn name, Map values) throws Exception {
      if (log.isTraceEnabled())
         log.trace("put " + name + " values=" + values);
      makeNode(name);
      if (values == null) {
         return;
      }
      Iterator i = values.entrySet().iterator();
      while (i.hasNext()) {
         Map.Entry me = (Map.Entry)i.next();
         Fqn rec = key(name, me.getKey());
         insert(rec, nullMask(me.getValue()));
      }
   }

   /**
    * Marks a FQN as a node.
    */
   private void makeNode(Fqn fqn) throws IOException {
      if (exists(fqn))
        return;
      int size = fqn.size();
      // TODO should not modify so darn often
      for (int i = size; i >= 0; i--) {
         Fqn child = fqn.getFqnChild(i);
         Object existing = tree.insert(child, NODE, false);
         if (existing != null)
           break;
      }
   }

   private Object insert(Fqn fqn, Object value) throws IOException {
      return nullUnmask( tree.insert(fqn, nullMask(value), true) );
   }

   /**
    * Erase a FQN and children.
    * Does not commit.
    */
   private void erase0(Fqn name)
      throws IOException
   {
      erase0(name, true);
   }

   private void erase0(Fqn name, boolean self)
      throws IOException
   {
      if (log.isTraceEnabled())
         log.trace("erase " + name + " self=" + self);
      synchronized (tree) {
         TupleBrowser browser = tree.browse(name);
         Tuple t = new Tuple();
         if (browser.getNext(t)) {
            if (self)
               tree.remove(t.getKey());
         }
         while (browser.getNext(t)) {
            Fqn fqn = (Fqn)t.getKey();
            if (!fqn.isChildOf(name))
               break;
            tree.remove(fqn);
         }
      }
   }

   /**
    * Erase a FQN's key.
    * Does not commit.
    */
   private Object eraseKey0(Fqn name, Object key)
      throws IOException
   {
      if (log.isTraceEnabled())
         log.trace("eraseKey " + name + " key " + key);
      Fqn fqnKey = key(name, key);
      try {
         return tree.remove(fqnKey);
      } catch (IllegalArgumentException e) {
         // Seems to be harmless
         // log.warn("IllegalArgumentException for " + fqnKey);
         // dump();
         return null;
      }
   }

   /**
    * Applies the given modifications.
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public void put(List modifications)
      throws Exception {

      checkOpen();
      checkNonNull(modifications, "modifications");

      apply(modifications);
      commit();
   }

   private void apply(List modifications)
      throws Exception
   {
      for (Iterator i = modifications.iterator(); i.hasNext();) {
         Modification mod = (Modification) i.next();
         Fqn name = mod.getFqn();
         Object oldVal;
         switch (mod.getType()) {
            case Modification.PUT_KEY_VALUE:
               oldVal = put0(name, mod.getKey(), mod.getValue());
               mod.setOldValue(oldVal);
               break;
            case Modification.PUT_DATA:
               put0(name, mod.getData());
               break;
            case Modification.PUT_DATA_ERASE:
               erase0(name);
               put0(name, mod.getData());
               break;
            case Modification.REMOVE_KEY_VALUE:
               oldVal = eraseKey0(name, mod.getKey());
               mod.setOldValue(oldVal);
               break;
            case Modification.REMOVE_NODE:
               erase0(name);
               break;
            case Modification.REMOVE_DATA:
               erase0(name, false);
               break;
            default:
               throw new IllegalArgumentException(
                     "Unknown Modification type: " + mod.getType());
         }
      }
   }

   /**
    * Deletes the node for a given FQN and all its descendent nodes.
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public void remove(Fqn name)
      throws Exception
   {
      erase0(name);
      commit();
   }

   /**
    * Deletes a single FQN-key-value record.
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public Object remove(Fqn name, Object key)
      throws Exception {

      try {
         return eraseKey0(name, key);
      } finally {
         commit();
      }
   }

   /**
    * Clears the map for the given node, but does not remove the node.
    */
   public void removeData(Fqn name)
      throws Exception
   {
      erase0(name, false);
   }

   /**
    * Applies and commits the given modifications in one transaction.
    */
   public void prepare(Object tx, List modifications, boolean onePhase)
      throws Exception
   {
      if (onePhase)
         put(modifications);
      else
         transactions.put(tx, modifications);
   }

   /**
    * Commits a transaction.
    */
   public void commit(Object tx) throws Exception {
      List modifications = (List)transactions.remove(tx);
      if (modifications == null)
         throw new IllegalStateException("transaction " + tx + " not found in transaction table");
      put(modifications);
      commit();
   }

   /**
    * Removes transaction in progress.
    */
   public void rollback(Object tx) {
      transactions.remove(tx);
   }

   public byte[] loadEntireState()
      throws Exception
   {
      return loadState(Fqn.ROOT);
   }

   /**
    * Export the contents of the databases as a byte array.
    * If the databases are empty a zero-lenth array is returned.
    */
   public byte[] loadState(Fqn subtree)
      throws Exception
   {
      ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
      
      try
      {
         // Set the TCCL to any classloader registered for subtree
         setUnmarshallingClassLoader(subtree);
         
         ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         ObjectOutputStream oos = new ObjectOutputStream(baos);
   
         synchronized (tree) {
            TupleBrowser browser = tree.browse(subtree);
            Tuple t = new Tuple();
            while (browser.getNext(t)) {
               Fqn fqn = (Fqn)t.getKey();
               if (!fqn.isChildOrEquals(subtree))
                  break;
               oos.writeObject(fqn);
               oos.writeObject(t.getValue());
            }
         }
         oos.flush();
         return baos.toByteArray();
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(currentCL);
      }
   }

   /**
    * Replace the contents of the databases with the given exported data.
    * If state is null or zero-length, the databases will be cleared.
    */
   public void storeEntireState(byte[] state)
      throws Exception
   {
      storeState(state, Fqn.ROOT);
   }

   /**
    * Replace the contents of the databases with the given exported data.
    * If state is null or zero-length, the databases will be cleared.
    */
   public void storeState(byte[] state, Fqn subtree) throws Exception 
   {
      ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
      try
      {
         // Set the TCCL to any classloader registered for subtree
         setUnmarshallingClassLoader(subtree);
         
         ByteArrayInputStream bais = new ByteArrayInputStream(state);
         ObjectInputStream ois = new ObjectInputStream(bais);
         
         erase0(subtree);
         
         boolean moveToBuddy = 
            subtree.isChildOf(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN) && subtree.size() > 1;
   
         // store new state
         Fqn storeFqn = null;         
         while (bais.available() > 0) {
            Fqn fqn = (Fqn)ois.readObject();
   
            if (moveToBuddy)
               storeFqn = BuddyManager.getBackupFqn(subtree, fqn);
            else
               storeFqn = fqn;
            
            Object value = ois.readObject();
            tree.insert(storeFqn, value, true);
         } 
         commit();
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(currentCL);
      }
   }

   /**
    * Throws an exception if the environment is not open.
    */
   private void checkOpen() {
      if (tree == null) {
         throw new IllegalStateException(
               "Operation not allowed before calling create()");
      }
   }

   /**
    * Throws an exception if the environment is not open.
    */
   private void checkNotOpen() {
      if (tree != null) {
         throw new IllegalStateException(
               "Operation not allowed after calling create()");
      }
   }

   /**
    * Throws an exception if the parameter is null.
    */
   private void checkNonNull(Object param, String paramName) {
      if (param == null) {
         throw new NullPointerException(
               "Parameter must not be null: " + paramName);
      }
   }

   private Object nullMask(Object o) {
     return (o == null) ? Null.NULL : o;
   }

   private Object nullUnmask(Object o) {
     return (o == Null.NULL) ? null : o;
   }

   /**
    * Dumps the tree to debug.
    */
   public void dump() throws IOException {
      dump(Fqn.ROOT);
   }

   /**
    * Dumps the tree past the key to debug.
    */
   public void dump(Object key) throws IOException {
      TupleBrowser browser = tree.browse(key);
      Tuple t = new Tuple();
      log.debug("contents: " + key);
      while (browser.getNext(t)) {
         log.debug(t.getKey() + "\t" + t.getValue());
      }
      log.debug("");
   }

   public void setRegionManager(RegionManager manager) {
      this.manager = manager;
   }

   private void setUnmarshallingClassLoader(Fqn subtree)
   {
      if (manager != null)
      {
         manager.setUnmarshallingClassLoader(subtree);
      }
   }

   public String toString()
   {
      BTree bt = tree;
      int size = (bt == null) ? -1 : bt.size(); 
      return "JdbmCacheLoader locationStr=" + locationStr + 
         " size=" + size;
   }

}
