/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader;

import org.jboss.cache.TreeCache;
import org.jboss.cache.Fqn;
import org.jboss.cache.config.CacheLoaderConfig;
import org.jboss.cache.marshall.RegionManager;

import java.util.*;

/**
 * This decorator is used whenever more than one cache loader is configured.  READ operations are directed to
 * each of the cache loaders (in the order which they were configured) until a non-null (or non-empty in the case
 * of retrieving collection objects) result is achieved.
 *
 * WRITE operations are propagated to ALL registered cacheloaders that specified set ignoreModifications to false.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class ChainingCacheLoader implements ExtendedCacheLoader
{
    private final List cacheLoaders = new ArrayList(2);
    private final List writeCacheLoaders = new ArrayList(2);
    private final List cacheLoaderConfigs = new ArrayList(2);

    /**
     * Sets the configuration. Will be called before {@link #create()} and {@link #start()}
     *
     * @param url A list of properties, defined in the XML file
     */
    public void setConfig(Properties url)
    {
        // don't do much here?
    }

    /**
     * This method allows the CacheLoader to set the TreeCache, therefore allowing the CacheLoader to invoke
     * methods of the TreeCache. It can also use the TreeCache to fetch configuration information. Alternatively,
     * the CacheLoader could maintain its own configuration<br/>
     * This method will be called directly after the CacheLoader instance has been created
     *
     * @param c The cache on which this loader works
     */
    public void setCache(TreeCache c)
    {
        // not much to do here?
    }

    /**
     * Returns a list of children names, all names are <em>relative</em>. Returns null if the parent node is not found.
     * The returned set must not be modified, e.g. use Collections.unmodifiableSet(s) to return the result
     *
     * @param fqn The FQN of the parent
     * @return Set<String>. A list of children. Returns null if no children nodes are present, or the parent is
     *         not present
     */
    public Set getChildrenNames(Fqn fqn) throws Exception
    {
        Set answer = null;
        Iterator i = cacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            answer = l.getChildrenNames(fqn);
            if (answer != null && answer.size() > 0) break;
        }
        return answer;
    }

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

    /**
     * Returns the value for a given key. Returns null if the node doesn't exist, or the value is not bound
     *
     * @param name
     * @return
     * @throws Exception
     */
//    public Object get(Fqn name, Object key) throws Exception
//    {
//        Object answer = null;
//        Iterator i = cacheLoaders.iterator();
//        while (i.hasNext())
//        {
//            CacheLoader l = (CacheLoader) i.next();
//            answer = l.get(name, key);
//            if (answer != null) break;
//        }
//        return answer;
//    }

    /**
     * Returns all keys and values from the persistent store, given a fully qualified name
     *
     * @param name
     * @return Map<Object,Object> of keys and values for the given node. Returns null if the node was not found, or
     *         if the node has no attributes
     * @throws Exception
     */
    public Map get(Fqn name) throws Exception
    {
        Map answer = null;
        Iterator i = cacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            answer = l.get(name);
            if (answer != null) break;
        }
        return answer;
    }

    /**
     * Checks whether the CacheLoader has a node with Fqn
     *
     * @param name
     * @return True if node exists, false otherwise
     */
    public boolean exists(Fqn name) throws Exception
    {
        boolean answer = false;
        Iterator i = cacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            answer = l.exists(name);
            if (answer) break;
        }
        return answer;
    }

    /**
     * Inserts key and value into the attributes hashmap of the given node. If the node does not exist, all
     * parent nodes from the root down are created automatically. Returns the old value
     */
    public Object put(Fqn name, Object key, Object value) throws Exception
    {
        Object answer = null;
        Iterator i = writeCacheLoaders.iterator();
        boolean isFirst = true;
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            Object tAnswer = l.put(name, key, value);
            if (isFirst)
            {
                answer = tAnswer;
                isFirst = false;
            }

        }
        return answer;
    }

    /**
     * Inserts all elements of attributes into the attributes hashmap of the given node, overwriting existing
     * attributes, but not clearing the existing hashmap before insertion (making it a union of existing and
     * new attributes)
     * If the node does not exist, all parent nodes from the root down are created automatically
     *
     * @param name       The fully qualified name of the node
     * @param attributes A Map of attributes. Can be null
     */
    public void put(Fqn name, Map attributes) throws Exception
    {
        Iterator i = writeCacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            l.put(name, attributes);
        }
    }

    /**
     * Inserts all modifications to the backend store. Overwrite whatever is already in
     * the datastore.
     *
     * @param modifications A List<Modification> of modifications
     * @throws Exception
     */
    public void put(List modifications) throws Exception
    {
        Iterator i = writeCacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            l.put(modifications);
        }
    }

    /**
     * Removes the given key and value from the attributes of the given node. No-op if node doesn't exist.
     * Returns the first response from the loader chain.
     */
    public Object remove(Fqn name, Object key) throws Exception
    {
        Object answer = null;
        Iterator i = writeCacheLoaders.iterator();
        boolean isFirst = true;
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            Object tAnswer = l.remove(name, key);
            if (isFirst)
            {
                answer = tAnswer;
                isFirst = false;
            }
        }
        return answer;
    }

    /**
     * Removes the given node. If the node is the root of a subtree, this will recursively remove all subnodes,
     * depth-first
     */
    public void remove(Fqn name) throws Exception
    {
        Iterator i = writeCacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            l.remove(name);
        }
    }

    /**
     * Removes all attributes from a given node, but doesn't delete the node itself
     *
     * @param name
     * @throws Exception
     */
    public void removeData(Fqn name) throws Exception
    {
        Iterator i = writeCacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            l.removeData(name);
        }
    }

    /**
     * Prepare the modifications. For example, for a DB-based CacheLoader:
     * <ol>
     * <li>Create a local (JDBC) transaction
     * <li>Associate the local transaction with <code>tx</code> (tx is the key)
     * <li>Execute the coresponding SQL statements against the DB (statements derived from modifications)
     * </ol>
     * For non-transactional CacheLoader (e.g. file-based), this could be a null operation
     *
     * @param tx            The transaction, just used as a hashmap key
     * @param modifications List<Modification>, a list of all modifications within the given transaction
     * @param one_phase     Persist immediately and (for example) commit the local JDBC transaction as well. When true,
     *                      we won't get a {@link #commit(Object)} or {@link #rollback(Object)} method call later
     * @throws Exception
     */
    public void prepare(Object tx, List modifications, boolean one_phase) throws Exception
    {
        Iterator i = writeCacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            l.prepare(tx, modifications, one_phase);
        }
    }

    /**
     * Commit the transaction. A DB-based CacheLoader would look up the local JDBC transaction asociated
     * with <code>tx</code> and commit that transaction<br/>
     * Non-transactional CacheLoaders could simply write the data that was previously saved transiently under the
     * given <code>tx</code> key, to (for example) a file system (note this only holds if the previous prepare() did
     * not define one_phase=true
     *
     * @param tx
     */
    public void commit(Object tx) throws Exception
    {
        Iterator i = writeCacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            l.commit(tx);
        }
    }

    /**
     * Roll the transaction back. A DB-based CacheLoader would look up the local JDBC transaction asociated
     * with <code>tx</code> and roll back that transaction
     *
     * @param tx
     */
    public void rollback(Object tx)
    {
        Iterator i = writeCacheLoaders.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            l.rollback(tx);
        }
    }

    /**
     * Fetch the entire state for this cache from secondary storage (disk, DB) and return it as a byte buffer.
     * This is for initialization of a new cache from a remote cache. The new cache would then call
     * storeEntireState().<br/>
     *
     * Only fetches state from the loader with fetchPersistentState as true.
     *
     * todo: define binary format for exchanging state
     */
    public byte[] loadEntireState() throws Exception
    {
        byte[] answer = null;
        Iterator i = cacheLoaders.iterator();
        Iterator cfgs = cacheLoaderConfigs.iterator();
        while (i.hasNext() && cfgs.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) cfgs.next();
            if (cfg.isFetchPersistentState())
            {
                answer = l.loadEntireState();
                break;
            }
        }
        return answer;
    }

    /**
     * Store the given state in secondary storage. Overwrite whatever is currently in secondary storage.
     *
     * Only stores this state in a loader that has fetchPersistentState as true.
     */
    public void storeEntireState(byte[] state) throws Exception
    {
        Iterator i = writeCacheLoaders.iterator();
        Iterator cfgs = cacheLoaderConfigs.iterator();
        while (i.hasNext())
        {
            CacheLoader l = (CacheLoader) i.next();
            CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) cfgs.next();
            if (cfg.isFetchPersistentState())
            {
                l.storeEntireState(state);
                break;
            }
        }
    }

    /**
     * Creates individual cache loaders.
     * @throws Exception
     */
    public void create() throws Exception
    {
        Iterator it = cacheLoaders.iterator();
        Iterator cfgIt = cacheLoaderConfigs.iterator();
        while (it.hasNext() && cfgIt.hasNext())
        {
            CacheLoader cl = (CacheLoader) it.next();
            CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) cfgIt.next();
            cl.create();
        }
    }

    public void start() throws Exception
    {
        Iterator it = cacheLoaders.iterator();
        while (it.hasNext())
        {
            ((CacheLoader) it.next()).start();
        }
    }

    public void stop()
    {
        Iterator it = cacheLoaders.iterator();
        while (it.hasNext())
        {
            ((CacheLoader) it.next()).stop();
        }
    }

    public void destroy()
    {
        Iterator it = cacheLoaders.iterator();
        while (it.hasNext())
        {
            ((CacheLoader) it.next()).destroy();
        }
    }
    
    
    // ---------------------------------------------------- ExtendedCacheLoader

    public byte[] loadState(Fqn subtree) throws Exception
    {
       byte[] answer = null;
       Iterator i = cacheLoaders.iterator();
       Iterator cfgs = cacheLoaderConfigs.iterator();
       while (i.hasNext() && cfgs.hasNext())
       {
           CacheLoader l = (CacheLoader) i.next();
           CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) cfgs.next();
           if (cfg.isFetchPersistentState())
           {
              if (l instanceof ExtendedCacheLoader)
              {
                 answer = ((ExtendedCacheLoader)l).loadState(subtree);
              }
              else
              {
                 throw new Exception("Cache loader " + l + 
                       " does not implement ExtendedCacheLoader");
              }
              break;
           }
       }
       return answer;
    }

    /**
     * No-op, as this class doesn't directly use the RegionManager.
     */
    public void setRegionManager(RegionManager manager)
    {
       // no-op -- we don't do anything with the region manager       
    }

    public void storeState(byte[] state, Fqn subtree) throws Exception
    {
       Iterator i = writeCacheLoaders.iterator();
       Iterator cfgs = cacheLoaderConfigs.iterator();
       while (i.hasNext())
       {
           CacheLoader l = (CacheLoader) i.next();
           CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) cfgs.next();
           if (cfg.isFetchPersistentState())
           {
              if (l instanceof ExtendedCacheLoader)
              {
                 ((ExtendedCacheLoader)l).storeState(state, subtree);
              }
              else
              {
                 throw new Exception("Cache loader " + l + 
                       " does not implement ExtendedCacheLoader");
              }
              break;
           }
       }
    }
    
    
    /**
     * Returns the number of cache loaders in the chain.
     */
    public int getSize()
    {
        return cacheLoaders.size();
    }

    /**
     * Returns a List<CacheLoader> of individual cache loaders configured.
     */
    public List getCacheLoaders()
    {
        return Collections.unmodifiableList(cacheLoaders);
    }

    /**
     * Adds a cache loader to the chain (always added at the end of the chain)
     * @param l the cache loader to add
     * @param cfg and its configuration
     */
    public void addCacheLoader(CacheLoader l, CacheLoaderConfig.IndividualCacheLoaderConfig cfg)
    {
        synchronized(this)
        {
            cacheLoaderConfigs.add(cfg);
            cacheLoaders.add(l);

            if (!cfg.isIgnoreModifications())
            {
                writeCacheLoaders.add(l);
            }
        }
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer("ChainingCacheLoader{");
        Iterator i = cacheLoaders.iterator();
        Iterator c = cacheLoaderConfigs.iterator();
        int count=0;
        while (i.hasNext() && c.hasNext())
        {
            CacheLoader loader = (CacheLoader) i.next();
            CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) c.next();

            buf.append(++count);
            buf.append(": IgnoreMods? ");
            buf.append(cfg.isIgnoreModifications());
            buf.append(" CLoader: ");
            buf.append(loader);
            buf.append("; ");
        }
        buf.append("}");
        return buf.toString();
    }

    public void purgeIfNecessary() throws Exception
    {
        Iterator loaders = cacheLoaders.iterator();
        Iterator configs = cacheLoaderConfigs.iterator();

        while (loaders.hasNext() && configs.hasNext())
        {
            CacheLoader myLoader = (CacheLoader) loaders.next();
            CacheLoaderConfig.IndividualCacheLoaderConfig myConfig = (CacheLoaderConfig.IndividualCacheLoaderConfig) configs.next();

            if (!myConfig.isIgnoreModifications() && myConfig.isPurgeOnStartup()) myLoader.remove(Fqn.ROOT);
        }


    }
}
