package org.jboss.cache.loader.bdbje;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.*;
import com.sleepycat.je.util.DbDump;
import com.sleepycat.je.util.DbLoad;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.TreeCache;
import org.jboss.cache.loader.CacheLoader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;


/**
 * A persistent <code>CacheLoader</code> based on Berkeley DB Java Edition.
 *
 * <p>The configuration string format is:</p>
 * <pre>environmentDirectoryName[#databaseName]</pre>
 * <p>where databaseName, if omitted, defaults to the ClusterName property
 * of the TreeCache.</p>
 *
 * <p>A je.properties file may optionally be placed in the JE environment
 * directory and used to customize the default JE configuration.</p>
 *
 * @author Mark Hayes May 16, 2004
 * @author Bela Ban
 * @version $Id: BdbjeCacheLoader.java 2574 2006-09-18 21:39:48Z bstansberry $
 */
public class BdbjeCacheLoader implements CacheLoader {

   private static final int MAX_TXN_RETRIES = 10;
   private static final char LOWEST_UTF_CHAR = '\u0001';

   private static final Log log = LogFactory.getLog(BdbjeCacheLoader.class);

   private String configStr;
   private TreeCache treeCache;
   private Environment env;
   private String cacheDbName;
   private String catalogDbName;
   private Database cacheDb;
   private Database catalogDb;
   private StoredClassCatalog catalog;
   private SerialBinding serialBinding;
   private Map txnMap;
   private boolean transactional;

   /*
    * Service implementation -- lifecycle methods.
    * Note that setConfig() and setCache() are called before create().
    */

   /**
    * Does nothing since start() does all the work.
    */
   public void create() throws Exception {
      String license="\n*************************************************************************************\n" +
            "Berkeley DB Java Edition version: " + JEVersion.CURRENT_VERSION.toString() + "\n" +
            "JBossCache can use Berkeley DB Java Edition from Sleepycat Software \n" +
            "(http://www.sleepycat.com/jeforjbosscache)\n" +
            "for persistent, reliable and transaction-protected data storage.\n" +
            "If you choose to use Berkeley DB Java Edition with JBossCache, you must comply with the terms\n" +
            "of Sleepycat's public license, included in the file LICENSE.txt.\n" +
            "If you prefer not to release the source code for your own application in order to comply\n" +
            "with the Sleepycat public license, you may purchase a different license for use of\n" +
            "Berkeley DB Java Edition with JBossCache.\n" +
            "Contact Sleepycat Software at info@sleepycat.com for pricing and terms for that license\n" +
            "*************************************************************************************";
      System.out.println(license);

      log.trace("Creating BdbjeCacheLoader instance.");
      checkNotOpen();
   }

   /**
    * Does nothing since stop() does all the work.
    */
   public void destroy() {
   }

   /**
    * Opens the JE environment and the database specified by the configuration
    * string.  The environment and databases are created if necessary.
    */
   public void start()
      throws Exception {

      log.trace("Starting BdbjeCacheLoader instance.");
      checkNotOpen();

      if (treeCache == null) {
         throw new IllegalStateException(
            "A non-null Cache property (TreeCache object) is required");
      }
      if (configStr == null) {
         configStr=System.getProperty("java.io.tmpdir");
      }

      // test location
      File location = new File(configStr);
      if (!location.exists())
      {
          boolean created = location.mkdirs();
          if (!created) throw new IOException("Unable to create cache loader location " + location);

      }
      if (!location.isDirectory()) throw new IOException("Cache loader location [" + location + "] is not a directory!");

      /* Parse config string. */
      File homeDir;
      int offset = configStr.indexOf('#');
      if (offset >= 0 && offset < configStr.length() - 1) {
         homeDir = new File(configStr.substring(0, offset));
         cacheDbName = configStr.substring(offset + 1);
      } else {
         homeDir = new File(configStr);
         cacheDbName = treeCache.getClusterName();
      }
      catalogDbName = cacheDbName + "_class_catalog";

      /*
       * If the TreeCache is transactional, we will create transactional
       * databases.  However, we always create a transactional environment
       * since it may be shared by transactional and non-transactional caches.
       */
      transactional = treeCache.getTransactionManager() != null;

      try {
         /* Open the environment, creating it if it doesn't exist. */
         EnvironmentConfig envConfig = new EnvironmentConfig();
         envConfig.setAllowCreate(true);
         envConfig.setTransactional(true);
         if (log.isTraceEnabled()) log.trace("Creating JE environment with home dir " + homeDir);
         env = new Environment(homeDir, envConfig);
         if (log.isDebugEnabled()) log.debug("Created JE environment " + env + " for cache loader " + this);
         /* Open cache and catalog databases. */
         openDatabases();
      } catch (Exception e) {
         destroy();
         throw e;
      }
   }

   /**
    * Opens all databases and initializes database related information.
    */
   private void openDatabases()
      throws Exception {

      /* Use a generic database config, with no duplicates allowed. */
      DatabaseConfig dbConfig  = new DatabaseConfig();
      dbConfig.setAllowCreate(true);
      dbConfig.setTransactional(transactional);

      /* Create/open the cache database and associated catalog database. */
      cacheDb = env.openDatabase(null, cacheDbName, dbConfig);
      catalogDb = env.openDatabase(null, catalogDbName, dbConfig);

      /* Use the catalog for the serial binding. */
      catalog = new StoredClassCatalog(catalogDb);
      serialBinding = new SerialBinding(catalog, null);

      /* Start with a fresh transaction map. */
      txnMap = new ConcurrentHashMap();
   }

   /**
    * Closes all databases, ignoring exceptions, and nulls references to all
    * database related information.
    */
   private void closeDatabases() {

      if (cacheDb != null) {
         try {
            cacheDb.close();
         } catch (Exception shouldNotOccur) {
            log.warn("Caught unexpected exception", shouldNotOccur);
         }
      }
      if (catalogDb != null) {
         try {
            catalogDb.close();
         } catch (Exception shouldNotOccur) {
            log.warn("Caught unexpected exception", shouldNotOccur);
         }
      }
      cacheDb = null;
      catalogDb = null;
      catalog = null;
      serialBinding = null;
      txnMap = null;
   }

   /**
    * Closes the JE databases and environment, and nulls references to them.
    * The environment and databases are not removed from the file system.
    * Exceptions during close are ignored.
    */
   public void stop() {

      closeDatabases();

      if (env != null) {
         try {
            env.close();
         } catch (Exception shouldNotOccur) {
            log.warn("Unexpected exception", shouldNotOccur);
         }
      }
      env = null;
   }

   /*
    * CacheLoader implementation.
    */

   /**
    * Sets the configuration string for this cache loader.
    */
   public void setConfig(Properties props) {
      checkNotOpen();
       configStr = props != null? props.getProperty("location") : null;
       if (log.isTraceEnabled()) log.trace("Configuring cache loader with location = " + configStr);
   }

   /**
    * Sets the TreeCache owner of this cache loader.
    */
   public void setCache(TreeCache c) {
      checkNotOpen();
      treeCache = c;
   }

   /**
    * Returns an unmodifiable set of relative children names (strings), or
    * returns null if the parent node is not found or if no children are found.
    * This is a fairly expensive operation, and is assumed to be performed by
    * browser applications.  Calling this method as part of a run-time
    * transaction is not recommended.
    */
   public Set getChildrenNames(Fqn name)
      throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      DatabaseEntry prefixEntry = makeKeyEntry(name);
      DatabaseEntry dataEntry = new DatabaseEntry();
      dataEntry.setPartial(0, 0, true);

      String namePart = "";
      int namePartIndex = name.size();
      Set set = null;

      Cursor cursor = cacheDb.openCursor(null, null);
      try {
         while (true) {
            DatabaseEntry keyEntry = makeKeyEntry(prefixEntry, namePart);
            OperationStatus status =
               cursor.getSearchKeyRange(keyEntry, dataEntry, null);
            if (status != OperationStatus.SUCCESS ||
                !startsWith(keyEntry, prefixEntry)) {
               break;
            }
            if (set == null) {
               set = new HashSet();
            }
            Fqn childName = makeKeyObject(keyEntry);
            namePart = childName.get(namePartIndex).toString();
            set.add(namePart);
            namePart += LOWEST_UTF_CHAR;
         }
      } finally {
         cursor.close();
      }
      if (set != null) {
         return Collections.unmodifiableSet(set);
      } else {
         return null;
      }
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

      DatabaseEntry keyEntry = makeKeyEntry(name);
      DatabaseEntry foundData = new DatabaseEntry();
      OperationStatus status = cacheDb.get(null, keyEntry, foundData, null);
      if (status == OperationStatus.SUCCESS) {
         //  changed createIfNull param to true
          // See http://jira.jboss.com/jira/browse/JBCACHE-118
          return makeDataObject(foundData, true);
      } else {
         return null;
      }
   }

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

    /**
    * Returns the data object stored under the given FQN and key, or null if
    * the FQN and key are not present.
    * This operation is always non-transactional, even in a transactional
    * environment.
    */
//   public Object get(Fqn name, Object key)
//      throws Exception {
//
//      Map map = get(name);
//      if (map != null) {
//         return map.get(key);
//      } else {
//         return null;
//      }
//   }

   /**
    * Returns whether the given node exists.
    */
   public boolean exists(Fqn name)
      throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      DatabaseEntry keyEntry = makeKeyEntry(name);
      DatabaseEntry foundData = new DatabaseEntry();
      foundData.setPartial(0, 0, true);
      OperationStatus status = cacheDb.get(null, keyEntry, foundData, null);
      return (status == OperationStatus.SUCCESS);
   }


   /**
    * Stores a single FQN-key-value record.
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public Object put(Fqn name, Object key, Object value) throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      Object oldVal;
      if (transactional) {
         Modification mod =
            new Modification(Modification.PUT_KEY_VALUE, name, key, value);
         commitModification(mod);
         oldVal = mod.getOldValue();
      } else {
         oldVal = doPut(null, name, key, value);
      }
      return oldVal;
   }


   /**
    * Internal version of store(String,Object,Object) that allows passing a
    * transaction.
    */
   private Object doPut(Transaction txn, Fqn name, Object key, Object value)
      throws Exception {

      Object oldVal = null;
      /* To update-or-insert, try putNoOverwrite first, then a RMW cycle. */
      Map map = new HashMap();
      map.put(key, value);
      DatabaseEntry dataEntry = makeDataEntry(map);
      DatabaseEntry keyEntry = makeKeyEntry(name);
      Cursor cursor = cacheDb.openCursor(txn, null);
      try {
         OperationStatus status=cursor.putNoOverwrite(keyEntry, dataEntry);
         if(status == OperationStatus.SUCCESS) {
            createParentNodes(cursor, name);
         }
         else {
            DatabaseEntry foundData=new DatabaseEntry();
            status=cursor.getSearchKey(keyEntry, foundData, LockMode.RMW);
            if(status == OperationStatus.SUCCESS) {
               map=makeDataObject(foundData, true);
               oldVal = map.put(key, value);
               cursor.putCurrent(makeDataEntry(map));
            }
         }
      }
      finally {
         cursor.close();
      }
      return oldVal;
   }



   /**
    * Stores a map of key-values for a given FQN, but does not delete existing
    * key-value pairs (that is, it does not erase).
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public void put(Fqn name, Map values)
      throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      if (transactional) {
         commitModification(
            new Modification(Modification.PUT_DATA, name, values));
      } else {
         doPut(null, name, values);
      }
   }

   /**
    * Internal version of put(Fqn,Map) that allows passing a
    * transaction.
    */
   private void doPut(Transaction txn, Fqn name, Map values)
      throws Exception {
      
      // JBCACHE-769 -- make a defensive copy
      values = (values == null ? null : new HashMap(values));

      /* To update-or-insert, try putNoOverwrite first, then a RMW cycle. */
      DatabaseEntry dataEntry = makeDataEntry(values);
      DatabaseEntry keyEntry = makeKeyEntry(name);
      Cursor cursor = cacheDb.openCursor(txn, null);
      try {
         OperationStatus status = cursor.putNoOverwrite(keyEntry, dataEntry);
         if (status == OperationStatus.SUCCESS) {
            createParentNodes(cursor, name);
         } else {
            DatabaseEntry foundData = new DatabaseEntry();
            status = cursor.getSearchKey(keyEntry, foundData, LockMode.RMW);
            if (status == OperationStatus.SUCCESS) {
               Map map = makeDataObject(foundData, true);
               if(values != null)
                  map.putAll(values);
               cursor.putCurrent(makeDataEntry(map));
            }
         }
      } finally {
         cursor.close();
      }
   }

   /**
    * Internal version of put(Fqn,Map) that allows passing a
    * transaction and erases existing data.
    */
   private void doPutErase(Transaction txn, Fqn name, Map values)
      throws Exception {
      
      // JBCACHE-769 -- make a defensive copy
      values = (values == null ? null : new HashMap(values));

      DatabaseEntry dataEntry = makeDataEntry(values);
      DatabaseEntry keyEntry = makeKeyEntry(name);
      Cursor cursor = cacheDb.openCursor(txn, null);
      try {
         cursor.put(keyEntry, dataEntry);
         createParentNodes(cursor, name);
      } finally {
         cursor.close();
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

      if (transactional) {
         commitModifications(modifications);
      } else {
         doPut(null, modifications);
      }
   }

   /**
    * Internal version of put(List) that allows passing a transaction.
    */
   private void doPut(Transaction txn, List modifications)
      throws Exception {

      /* This could be optimized by grouping modifications by Fqn, and
       * performing a single database operation for each Fqn (record). */

      for (Iterator i = modifications.iterator(); i.hasNext();) {
         Modification mod = (Modification) i.next();
         Fqn name = mod.getFqn();
         Object oldVal;
         switch (mod.getType()) {
            case Modification.PUT_KEY_VALUE:
               oldVal = doPut(txn, name, mod.getKey(), mod.getValue());
               mod.setOldValue(oldVal);
               break;
            case Modification.PUT_DATA:
               doPut(txn, name, mod.getData());
               break;
            case Modification.PUT_DATA_ERASE:
               doPutErase(txn, name, mod.getData());
               break;
            case Modification.REMOVE_KEY_VALUE:
               oldVal = doRemove(txn, name, mod.getKey());
               mod.setOldValue(oldVal);
               break;
            case Modification.REMOVE_NODE:
               doRemove(txn, name);
               break;
            case Modification.REMOVE_DATA:
               doRemoveData(txn, name);
               break;
            default:
               throw new IllegalArgumentException(
                     "Unknown Modification type: " + mod.getType());
         }
      }
   }

   /**
    * Creates parent nodes of the given Fqn, moving upward until an existing
    * node is found.
    */
   private void createParentNodes(Cursor cursor, Fqn name)
      throws Exception {

      DatabaseEntry dataEntry = makeDataEntry(null);
      for (int nParts = name.size() - 1; nParts >= 1; nParts -= 1) {
         DatabaseEntry keyEntry = makeKeyEntry(name, nParts);
         OperationStatus status = cursor.putNoOverwrite(keyEntry, dataEntry);
         if (status != OperationStatus.SUCCESS) {
            break;
         }
      }
   }

   /**
    * Deletes the node for a given FQN and all its descendent nodes.
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public void remove(Fqn name)
      throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      if (transactional) {
         commitModification(
            new Modification(Modification.REMOVE_NODE, name));
      } else {
         doRemove(null, name);
      }
   }

   /**
    * Internal version of remove(Fqn) that allows passing a transaction.
    */
   private void doRemove(Transaction txn, Fqn name)
      throws Exception {

      DatabaseEntry keyEntry = makeKeyEntry(name);
      DatabaseEntry foundKey = new DatabaseEntry();
      DatabaseEntry foundData = new DatabaseEntry();
      foundData.setPartial(0, 0, true);
      Cursor cursor = cacheDb.openCursor(txn, null);
      try {
         OperationStatus status =
            cursor.getSearchKey(keyEntry, foundData, LockMode.RMW);
         while (status == OperationStatus.SUCCESS) {
            cursor.delete();
            status = cursor.getNext(foundKey, foundData, LockMode.RMW);
            if (status == OperationStatus.SUCCESS &&
                !startsWith(foundKey, keyEntry)) {
               status = OperationStatus.NOTFOUND;
            }
         }
      } finally {
         cursor.close();
      }
   }

   /**
    * Deletes a single FQN-key-value record.
    * Intended to be used in a non-transactional environment, but will use
    * auto-commit in a transactional environment.
    */
   public Object remove(Fqn name, Object key)
      throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      Object oldVal;
      if (transactional) {
         Modification mod =
            new Modification(Modification.REMOVE_KEY_VALUE, name, key);
         commitModification(mod);
         oldVal = mod.getOldValue();
      } else {
         oldVal = doRemove(null, name, key);
      }
      return oldVal;
   }

   /**
    * Internal version of remove(String,Object) that allows passing a
    * transaction.
    */
   private Object doRemove(Transaction txn, Fqn name, Object key)
      throws Exception {

      Object oldVal = null;
      DatabaseEntry keyEntry = makeKeyEntry(name);
      DatabaseEntry foundData = new DatabaseEntry();
      Cursor cursor = cacheDb.openCursor(txn, null);
      try {
         OperationStatus status =
            cursor.getSearchKey(keyEntry, foundData, LockMode.RMW);
         if (status == OperationStatus.SUCCESS) {
            Map map = makeDataObject(foundData, true);
            oldVal = map.remove(key);
            cursor.putCurrent(makeDataEntry(map));
         }
      } finally {
         cursor.close();
      }
      return oldVal;
   }

   /**
    * Clears the map for the given node, but does not remove the node.
    */
   public void removeData(Fqn name)
      throws Exception {

      checkOpen();
      checkNonNull(name, "name");

      if (transactional) {
         commitModification(
            new Modification(Modification.REMOVE_DATA, name));
      } else {
         doRemoveData(null, name);
      }
   }

   /**
    * Internal version of removeData(Fqn) that allows passing a transaction.
    */
   private void doRemoveData(Transaction txn, Fqn name)
      throws Exception {

      DatabaseEntry dataEntry = new DatabaseEntry();
      dataEntry.setPartial(0, 0, true);
      DatabaseEntry keyEntry = makeKeyEntry(name);
      Cursor cursor = cacheDb.openCursor(txn, null);
      try {
         OperationStatus status =
            cursor.getSearchKey(keyEntry, dataEntry, LockMode.RMW);
         if (status == OperationStatus.SUCCESS) {
            cursor.putCurrent(makeDataEntry(null));
         }
      } finally {
         cursor.close();
      }
   }

   /**
    * Begins a transaction and applies the given modifications.
    *
    * <p>If onePhase is true, commits the transaction; otherwise, associates
    * the txn value with the transaction and expects commit() or rollback() to
    * be called later with the same tx value.  Performs retries if necessary to
    * resolve deadlocks.</p>
    */
   public void prepare(Object tx, List modifications, boolean onePhase)
      throws Exception {

      checkOpen();
      checkNonNull(modifications, "modifications");
      if (!onePhase) {
         checkNonNull(tx, "tx");
      }
      if (!transactional) {
         throw new UnsupportedOperationException(
            "prepare() not allowed with a non-transactional cache loader");
      }
      Transaction txn = performTransaction(modifications);
      if (onePhase) {
         txn.commit();
      } else {
         txnMap.put(tx, txn);
      }
   }

   /**
    * Performs and commits a single modification.  The loader must be
    * transactional. Commits the transaction if successful, or aborts the
    * transaction and throws an exception if not successful.
    */
   private void commitModification(Modification mod)
      throws Exception {

      commitModifications(Collections.singletonList(mod));
   }

   /**
    * Performs and commits a list of modifications.  The loader must be
    * transactional. Commits the transaction if successful, or aborts the
    * transaction and throws an exception if not successful.
    */
   private void commitModifications(List mods)
      throws Exception {

      if (!transactional) throw new IllegalStateException();
      Transaction txn = performTransaction(mods);
      txn.commit();
   }

   /**
    * Performs the given operation, starting a transaction and performing
    * retries.  Returns the transaction if successful; aborts the transaction
    * and throws an exception if not successful.
    */
   private Transaction performTransaction(List modifications)
      throws Exception {

      /*
       * Note that we can't use TransactionRunner here since if onePhase=false
       * in the call to prepare(), we do not want to commit.  TransactionRunner
       * always commits or aborts.
       */

      int retries = MAX_TXN_RETRIES;
      while (true) {
         Transaction txn = env.beginTransaction(null, null);
         try {
            doPut(txn, modifications);
            return txn;
         } catch (Exception e) {
            txn.abort();
            if (e instanceof DeadlockException && retries > 0) {
               retries -= 1;
            } else {
               throw e;
            }
         }
      }
   }

   /**
    * Commits the given transaction, or throws IllegalArgumentException if the
    * given key is not associated with an uncommited transaction.
    */
   public void commit(Object tx)
      throws Exception {

      checkOpen();
      checkNonNull(tx, "tx");

      Transaction txn = (Transaction) txnMap.remove(tx);
      if (txn != null) {
         txn.commit();
      } else if (transactional) {
         throw new IllegalArgumentException("Unknown txn key: " + tx);
      }
   }

   /**
    * Commits the given transaction, or throws IllegalArgumentException if the
    * given key is not associated with an uncommited transaction.
    */
   public void rollback(Object tx) {

      checkOpen();
      checkNonNull(tx, "tx");

      Transaction txn = (Transaction) txnMap.remove(tx);
      if (txn != null) {
         try {
            txn.abort();
         } catch (Exception ignored) {}
      } else if (transactional) {
         throw new IllegalArgumentException("Unknown txn key: " + tx);
      }
   }

   /**
    * Export the contents of the databases as a byte array.
    * If the databases are empty a zero-lenth array is returned.
    * The export format is two consecutive DbDumps.
    */
   public byte[] loadEntireState()
      throws Exception {

      checkOpen();

      boolean isEmpty;
      Cursor cursor = cacheDb.openCursor(null, null);
      try {
         isEmpty = cursor.getFirst(new DatabaseEntry(),
                                   new DatabaseEntry(),
                                   null) != OperationStatus.SUCCESS;
      } finally {
         cursor.close();
      }
      if (isEmpty) {
         return new byte[0];
      } else {
         ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
         PrintStream ps = new PrintStream(baos, false, "US-ASCII");
         dumpDatabase(cacheDbName, ps);
         dumpDatabase(catalogDbName, ps);
         ps.flush();
         return baos.toByteArray();
      }
   }

   /**
    * Dumps one database in ASCII format and adds a DATA=END marker line.
    */
   private void dumpDatabase(String dbName, PrintStream ps)
      throws Exception {

      DbDump dumper = new DbDump(env, dbName, ps, false);
      dumper.dump();
   }

   /**
    * Replace the contents of the databases with the given exported data.
    * If state is null or zero-length, the databases will be cleared.
    * The export format is two consecutive DbDumps.
    */
   public void storeEntireState(byte[] state)
      throws Exception {

      checkOpen();

      closeDatabases();
      env.removeDatabase(null, cacheDbName);
      env.removeDatabase(null, catalogDbName);
      if (state != null && state.length > 0) {
         BufferedReader reader = new BufferedReader
            (new InputStreamReader(new ByteArrayInputStream(state)));
         loadDatabase(cacheDbName, reader);
         loadDatabase(catalogDbName, reader);
      }
      openDatabases();
   }

   /**
    * Loads one database in ASCII format, stopping at the DATA=END marker line
    * or EOF.
    */
   private void loadDatabase(String dbName, BufferedReader reader)
      throws Exception {

      DbLoad loader = new DbLoad();
      loader.setEnv(env);
      loader.setDbName(dbName);
      loader.setInputReader(reader);
      loader.setNoOverwrite(false);
      loader.setTextFileMode(false);
      loader.setIgnoreUnknownConfig(true);
      loader.load();
   }

   /**
    * Returns whether the given entry starts with the given prefix bytes.
    * Used to determine whether a database key starts with a given FQN.
    */
   private boolean startsWith(DatabaseEntry entry,
                              DatabaseEntry prefix) {
      int size = prefix.getSize();
      if (size > entry.getSize()) {
         return false;
      }
      byte[] d1 = entry.getData();
      byte[] d2 = prefix.getData();
      int o1 = entry.getOffset();
      int o2 = prefix.getOffset();
      for (int i = 0; i < size; i += 1) {
         if (d1[o1 + i] != d2[o2 + i]) {
            return false;
         }
      }
      return true;
   }

   /**
    * Converts a database entry to an Fqn.
    */
   private Fqn makeKeyObject(DatabaseEntry entry) {

      Fqn name = Fqn.ROOT;
      TupleInput tupleInput = TupleBinding.entryToInput(entry);
      while (tupleInput.available() > 0) {
         String part = tupleInput.readString();
         name=new Fqn(name, part);
      }
      return name;
   }

   /**
    * Converts an Fqn to a database entry.
    */
   private DatabaseEntry makeKeyEntry(Fqn name) {

      return makeKeyEntry(name, name.size());
   }

   /**
    * Converts an Fqn to a database entry, outputing the given number of name
    * parts.
    */
   private DatabaseEntry makeKeyEntry(Fqn name, int nParts) {

      /* Write the sequence of name parts. */
      TupleOutput tupleOutput = new TupleOutput();
      for (int i = 0; i < nParts; i += 1) {
         tupleOutput.writeString(name.get(i).toString());
      }

      /* Return the tuple as an entry. */
      DatabaseEntry entry = new DatabaseEntry();
      TupleBinding.outputToEntry(tupleOutput, entry);
      return entry;
   }

   /**
    * Creates a key database entry from a parent database entry (prefix) and
    * a child name part.
    */
   private DatabaseEntry makeKeyEntry(DatabaseEntry prefix, String namePart) {

      /* Write the bytes of the prefix followed by the child name. */
      TupleOutput tupleOutput = new TupleOutput();
      tupleOutput.writeFast(prefix.getData(),
                            prefix.getOffset(),
                            prefix.getSize());
      tupleOutput.writeString(namePart);

      /* Return the tuple as an entry. */
      DatabaseEntry entry = new DatabaseEntry();
      TupleBinding.outputToEntry(tupleOutput, entry);
      return entry;
   }

   /**
    * Converts a database entry to a Map.
    */
   private Map makeDataObject(DatabaseEntry entry, boolean createIfNull) {
      Map map = (Map) serialBinding.entryToObject(entry);
      if (createIfNull && map == null) {
         map = new HashMap();
      }
      return map;
   }

   /**
    * Converts a Map to a database entry.
    */
   private DatabaseEntry makeDataEntry(Map map) {

      if (map != null) {
         if (map.size() == 0) {
            map = null;
         } else if (!(map instanceof Serializable)) {
            map = new HashMap(map);
         }
      }
      DatabaseEntry entry = new DatabaseEntry();
      serialBinding.objectToEntry(map, entry);
      return entry;
   }

   /**
    * Throws an exception if the environment is not open.
    */
   private void checkOpen() {
      if (env == null) {
         throw new IllegalStateException(
               "Operation not allowed before calling create()");
      }
   }

   /**
    * Throws an exception if the environment is not open.
    */
   private void checkNotOpen() {
      if (env != null) {
         throw new IllegalStateException(
               "Operation not allowed after calling create()");
      }
   }

   /**
    * Throws an exception if the parameter is null.
    */
   private void checkNonNull(Object param, String paramName) {
      if (param ==  null) {
         throw new NullPointerException(
               "Parameter must not be null: " + paramName);
      }
   }
}
