package org.jboss.cache.loader;

import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.system.Service;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A <code>CacheLoader</code> implementation persists and load keys to and from
 * secondary storage, such as a database or filesystem.  Typically,
 * implementations store a series of keys and values (an entire {@link Map})
 * under a single {@link Fqn}.  Loading and saving properties of an entire
 * {@link Map} should be atomic. 
 * <p/>
 * Lifecycle: First an instance of the loader is created, then the
 * configuration ({@link #setConfig(java.util.Properties)} ) and cache ({@link
 * #setCache(TreeCache)}) are set. After this, {@link #create()} is called.
 * Then {@link #start()} is called. When re-deployed, {@link #stop()} will be
 * called, followed by another {@link #start()}. Finally, when shut down,
 * {@link #destroy()} is called, after which the loader is unusable.
 *
 * It is important to note that all implementations are thread safe, as concurrent reads and writes, potentially even to
 * the same {@link Fqn}, are possible.
 *
 * @author Bela Ban Oct 31, 2003
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @version $Id: CacheLoader.java 4089 2007-06-29 13:33:38Z gzamarreno $
 */
public interface CacheLoader extends Service {


   /**
    * Sets the configuration.  This is called before {@link #create()} and {@link #start()}.
    * @param properties a collection of configuration properties
    */
   void setConfig(Properties properties);


   /**
    * Sets the {@link TreeCache} that is maintaining this CacheLoader.
    * This method allows this CacheLoader to invoke methods on TreeCache,
    * including fetching additional configuration information.  This method is
    * called be called after the CacheLoader instance has been constructed.
    * @param c The cache on which this loader works
    */
   void setCache(TreeCache c);


   /**
    * Returns a set of children node names as Strings.
    * All names are <em>relative</em> to this parent {@link Fqn}.
    * Returns null if the named node is not found or there are no children.
    * The returned set must not be modifiable.  Implementors can use
    * {@link java.util.Collections#unmodifiableSet(Set)} to make the set unmodifiable.
    *
    * @param fqn The FQN of the parent
    * @return Set<String> a set of children.  Returns null if no children nodes are
    * present, or the parent is not present
    */
   Set getChildrenNames(Fqn fqn) throws Exception;


   /**
    * Returns the value for a given key.  Returns null if the node doesn't
    * exist, or the value is null itself.
    */
//   Object get(Fqn name, Object key) throws Exception;


   /**
    * Returns all keys and values from the persistent store, given a fully qualified name.
    *
    * NOTE that the expected return value of this method has changed from JBossCache 1.2.x
    * and before!  This will affect cache loaders written prior to JBossCache 1.3.0 and such
    * implementations should be checked for compliance with the behaviour expected.
    *
    * @param name
    * @return Map<Object,Object> keys and values for the given node. Returns
    * null if the node is not found.  If the node is found but has no
    * attributes, this method returns an empty Map.
    * @throws Exception
    */
   Map get(Fqn name) throws Exception;


   /**
    * Returns true if the CacheLoader has a node with a {@link Fqn}.
    * @return true if node exists, false otherwise
    */
   boolean exists(Fqn name) throws Exception;


   /**
    * Puts a key and value into the attribute map of a given node.  If the
    * node does not exist, all parent nodes from the root down are created
    * automatically.  Returns the old value.
    */
   Object put(Fqn name, Object key, Object value) throws Exception;

   /**
    * Puts all entries of the map into the existing map of the given node,
    * overwriting existing keys, but not clearing the existing map before
    * insertion.
    * This is the same behavior as {@link Map#putAll}.
    * If the node does not exist, all parent nodes from the root down are created automatically
    * @param name The fully qualified name of the node
    * @param attributes A Map of attributes. Can be null
    */
   void put(Fqn name, Map attributes) throws Exception;



   /**
    * Applies all modifications to the backend store. 
    * Changes may be applied in a single operation.
    *
    * @param modifications A List<Modification> of modifications
    * @throws Exception
    */
   void put(List modifications) throws Exception;


   /**
    * Removes the given key and value from the attributes of the given node. 
    * Does nothing if the node doesn't exist
    * Returns the removed value.
    */
   Object remove(Fqn name, Object key) throws Exception;

   /**
    * Removes the given node and all its subnodes.  
    */
   void remove(Fqn name) throws Exception;


   /**
    * Removes all attributes from a given node, but doesn't delete the node
    * itself or any subnodes.
    */
   void removeData(Fqn name) throws Exception;


   /**
    * Prepares a list of modifications. For example, for a DB-based CacheLoader:
    * <ol>
    * <li>Create a local (JDBC) transaction
    * <li>Associate the local transaction with <code>tx</code> (tx is the key)
    * <li>Execute the coresponding SQL statements against the DB (statements derived from modifications)
    * </ol>
    * For non-transactional CacheLoader (e.g. file-based), this could be a null operation.
    *
    * @param tx            The transaction, just used as a hashmap key
    * @param modifications List<Modification>, a list of all modifications within the given transaction
    * @param one_phase     Persist immediately and (for example) commit the local JDBC transaction as well. When true,
    *                      we won't get a {@link #commit(Object)} or {@link #rollback(Object)} method call later
    * @throws Exception
    */
   void prepare(Object tx, List modifications, boolean one_phase) throws Exception;

   /**
    * Commits the transaction. A DB-based CacheLoader would look up the local
    * JDBC transaction asociated with <code>tx</code> and commit that
    * transaction.  Non-transactional CacheLoaders could simply write the data
    * that was previously saved transiently under the given <code>tx</code>
    * key, to (for example) a file system (note this only holds if the previous
    * prepare() did not define one_phase=true
    *
    * @param tx transaction to commit
    */
   void commit(Object tx) throws Exception;

   /**
    * Rolls the transaction back. A DB-based CacheLoader would look up the
    * local JDBC transaction asociated with <code>tx</code> and roll back that
    * transaction.
    *
    * @param tx transaction to roll back
    */
   void rollback(Object tx);


   /**
    * Fetches the entire state for this cache from secondary storage (disk, DB)
    * and returns it as a byte buffer.  This is for initialization of a new
    * cache from a remote cache. The new cache would then call
    * {@link #storeEntireState}.
    */
   byte[] loadEntireState() throws Exception;

   /**
    * Stores the given state in secondary storage. Overwrites whatever is
    * currently in storage.
    */
   void storeEntireState(byte[] state) throws Exception;

}
