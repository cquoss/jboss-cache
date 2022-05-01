/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.config.CacheLoaderConfig;
import org.jboss.cache.xml.XmlHelper;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Manages all cache loader funxtionality.  This class is typically initialised with an XML DOM Element,
 * represeting a cache loader configuration, or a {@see CacheLoaderConfig} object.
 *
 * Usage:
 *
 * <code>
 *  CacheLoaderManager manager = new CacheLoaderManager();
 *  manager.setConfig(myXmlSnippet, myTreeCache);
 *  CacheLoader loader = manager.getCacheLoader();
 * </code>
 *
 * The XML configuration passed in would typically look like:
 *
 * <code><![CDATA[

    <config>
        <passivation>false</passivation>
        <preload>/</preload>

        <cacheloader>
            <class>org.jboss.cache.loader.FileCacheLoader</class>
            <async>true</async>
            <fetchPersistentState>false</fetchPersistentState>
            <ignoreModifications>false</ignoreModifications>
            <properties>
                location=/tmp/file
            </properties>
        </cacheloader>
    </config>
 ]]>
 </code>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class CacheLoaderManager
{
    private static Log log = LogFactory.getLog(CacheLoaderManager.class);
    private CacheLoaderConfig config;
    private TreeCache cache;
    private CacheLoader loader;
    private boolean fetchPersistentState;
    private boolean extendedCacheLoader = true;

    /**
     * Creates a cache loader based on an XML element passed in.
     * @param cacheLoaderConfig
     * @throws Exception
     */
    public void setConfig(Element cacheLoaderConfig, TreeCache cache) throws Exception
    {
        this.cache = cache;
        config = new CacheLoaderConfig();
        config.setPassivation( XmlHelper.readBooleanContents(cacheLoaderConfig, "passivation"));
        config.setPreload( XmlHelper.readStringContents(cacheLoaderConfig, "preload"));
        config.setShared(XmlHelper.readBooleanContents(cacheLoaderConfig, "shared"));
        NodeList cacheLoaderNodes = cacheLoaderConfig.getElementsByTagName("cacheloader");
        for (int i=0; i<cacheLoaderNodes.getLength(); i++)
        {
            Node node = cacheLoaderNodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE)
            {
                Element element = (Element) node;
                CacheLoaderConfig.IndividualCacheLoaderConfig clc = new CacheLoaderConfig.IndividualCacheLoaderConfig();
                clc.setAsync(XmlHelper.readBooleanContents(element, "async", false));
                clc.setIgnoreModifications(XmlHelper.readBooleanContents(element, "ignoreModifications", false));
                clc.setFetchPersistentState(XmlHelper.readBooleanContents(element, "fetchPersistentState", false));
                clc.setPurgeOnStartup(XmlHelper.readBooleanContents(element, "purgeOnStartup", false));
                clc.setClassName(XmlHelper.readStringContents(element, "class"));
                clc.setProperties(XmlHelper.readPropertiesContents(element, "properties"));
                config.addIndividualCacheLoaderConfig(clc);
            }
        }
        loader = createCacheLoader();
    }

    /**
     * Sets a configuration object and creates a cacheloader accordingly.
     * @param config
     * @param cache
     * @throws Exception
     */
    public void setConfig(CacheLoaderConfig config, TreeCache cache) throws Exception
    {
        this.config = config == null ? new CacheLoaderConfig() : config;
        this.cache = cache;
        loader = createCacheLoader();
    }

    /**
     * Creates the cache loader based on a cache loader config passed in.
     * @return a configured cacheloader
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws ClassNotFoundException
     */
    private CacheLoader createCacheLoader() throws Exception
    {
        CacheLoader tmpLoader = null;
        // if we only have a single cache loader configured in the chaining cacheloader then
        // don't use a chaining cache loader at all.

        // also if we are using passivation then just directly use the first cache loader.
        if (config.useChainingCacheLoader())
        {
            // create chaining cache loader.
            tmpLoader = new ChainingCacheLoader();
            ChainingCacheLoader ccl = (ChainingCacheLoader) tmpLoader;
            Iterator it = config.getIndividualCacheLoaderConfigs().iterator();

            // only one cache loader may have fetchPersistentState to true.
            int numLoadersWithFetchPersistentState = 0;
            while (it.hasNext())
            {
                CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) it.next();
                if (cfg.isFetchPersistentState())
                {
                    numLoadersWithFetchPersistentState++;
                    fetchPersistentState = true;
                }
                if (numLoadersWithFetchPersistentState > 1) throw new Exception("Invalid cache loader configuration!!  Only ONE cache loader may have fetchPersistentState set to true.  Cache will not start!");
                CacheLoader l = createCacheLoader(cfg, cache);
                // Only loaders that deal w/ state transfer factor into
                // whether the overall chain supports ExtendedCacheLoader
                if (cfg.isFetchPersistentState())
                {
                   extendedCacheLoader = extendedCacheLoader && (l instanceof ExtendedCacheLoader);
                }
                ccl.addCacheLoader(l, cfg);
            }
        }
        else
        {
            CacheLoaderConfig.IndividualCacheLoaderConfig cfg = (CacheLoaderConfig.IndividualCacheLoaderConfig) config.getIndividualCacheLoaderConfigs().get(0);
            tmpLoader = createCacheLoader(cfg, cache);
            fetchPersistentState = cfg.isFetchPersistentState();
            extendedCacheLoader = (tmpLoader instanceof ExtendedCacheLoader);
        }

        return tmpLoader;
    }

    /**
     * Creates the cache loader based on the configuration.
     * @param cfg
     * @param cache
     * @return a cache loader
     * @throws Exception
     */
    private CacheLoader createCacheLoader(CacheLoaderConfig.IndividualCacheLoaderConfig cfg, TreeCache cache) throws Exception
    {
        // create loader
        CacheLoader tmpLoader = createInstance(cfg.getClassName());

        if (tmpLoader != null)
        {
            // async?
            if (cfg.isAsync())
            {
                CacheLoader asyncDecorator;
                if (tmpLoader instanceof ExtendedCacheLoader)
                {

                    asyncDecorator = new AsyncExtendedCacheLoader((ExtendedCacheLoader) tmpLoader);
                }
                else
                {
                    asyncDecorator = new AsyncCacheLoader(tmpLoader);
                }
                tmpLoader = asyncDecorator;
            }

            // load props
            tmpLoader.setConfig(cfg.getProperties());

            tmpLoader.setCache(cache);
            // we should not be creating/starting the cache loader here - this should be done in the separate
            // startCacheLoader() method.
//           tmpLoader.create();
//           tmpLoader.start();
            if (cache != null && cache.getUseMarshalling() && tmpLoader instanceof ExtendedCacheLoader)
            {
                ((ExtendedCacheLoader) tmpLoader).setRegionManager(cache.getRegionManager());
            }
        }
        return tmpLoader;
    }

    private CacheLoader createInstance(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class cl = Thread.currentThread().getContextClassLoader().loadClass(className);
        return (CacheLoader) cl.newInstance();
    }

    /**
     * Performs a preload on the cache based on the cache loader preload configs used when configuring the cache.
     * @throws Exception
     */
    public void preloadCache() throws Exception
    {
        if (config.getPreload() == null || config.getPreload().equals("")) return;
        if (log.isDebugEnabled()) log.debug("preloading transient state from cache loader " + loader);
        StringTokenizer st = new StringTokenizer(config.getPreload(), ",");
        String tok;
        Fqn fqn;
        long start, stop, total;
        start = System.currentTimeMillis();
        while (st.hasMoreTokens())
        {
            tok = st.nextToken();
            fqn = Fqn.fromString(tok.trim());
            if (log.isTraceEnabled()) log.trace("preloading " + fqn);
            preload(fqn, true, true);
        }

        stop = System.currentTimeMillis();
        total = stop - start;
        if (log.isDebugEnabled()) log.debug("preloading transient state from cache loader was successful (in " + total + " milliseconds)");
    }

    /**
     * Preloads a specific Fqn into the cache from the configured cacheloader
     * @param fqn fqn to preload
     * @param preloadParents whether we preload parents
     * @param preloadChildren whether we preload children
     * @throws Exception
     */
    public void preload(Fqn fqn, boolean preloadParents, boolean preloadChildren) throws Exception
    {

        // 1. Load the attributes first
        //  but this will go down the entire damn chain!!  :S
        cache.get(fqn, "bla");

        // 2. Then load the parents
        if (preloadParents)
        {
            Fqn tmp_fqn = Fqn.ROOT;
            for (int i = 0; i < fqn.size() - 1; i++)
            {
                tmp_fqn = new Fqn(tmp_fqn, fqn.get(i));
                cache.get(tmp_fqn, "bla");
            }
        }

        if (preloadChildren)
        {
            // 3. Then recursively for all child nodes, preload them as well
            Set children = loader.getChildrenNames(fqn);
            if (children != null)
            {
                for (Iterator it = children.iterator(); it.hasNext();)
                {
                    String child_name = (String) it.next();
                    Fqn child_fqn = new Fqn(fqn, child_name);
                    preload(child_fqn, false, true);
                }
            }
        }
    }

    /**
     * Returns the configuration element of the cache loaders
     */
    public CacheLoaderConfig getCacheLoaderConfig()
    {
        return config;
    }

    /**
     * Returns the cache loader
     */
    public CacheLoader getCacheLoader()
    {
        return loader;
    }

    /**
     * Overrides generated cache loader with the one provided,for backward compat.
     * @param loader
     */
    public void setCacheLoader(CacheLoader loader)
    {
        this.loader = loader;
    }

    /**
     * Tests if we're using passivation
     */
    public boolean isPassivation()
    {
        return config.isPassivation();
    }

    /**
     * Returns true if at least one of the configured cache loaders has set fetchPersistentState to true.
     */
    public boolean isFetchPersistentState()
    {
        return fetchPersistentState;
    }

   /**
    * Gets whether the cache loader supports the {@link ExtendedCacheLoader} API.
    * 
    * @return <code>true</code> if the cache loader implements ExtendedCacheLoader,
    *         or, if the cache loader is a {@link ChainingCacheLoader}, all
    *         cache loaders in the chain whose <code>FetchPersistentState</code>
    *         property is <code>true</code> implement ExtendedCacheLoader.
    */
   public boolean isExtendedCacheLoader()
   {
      return extendedCacheLoader;
   }

   public void stopCacheLoader()
    {
        if (loader == null) throw new RuntimeException("Problem with configured cache loader - it has been set to null!");
        // stop the cache loader
        loader.stop();
        // destroy the cache loader
        loader.destroy();
    }

    public void startCacheLoader() throws Exception
    {
        if (loader == null) throw new RuntimeException("Improperly configured cache loader - cache loader is null!");
        // create the cache loader
        loader.create();
        // start the cache loader
        loader.start();

        purgeLoaders(false);
    }

    public void purgeLoaders(boolean force) throws Exception
    {
        if ((loader instanceof ChainingCacheLoader) && !force)
        {
            ((ChainingCacheLoader) loader).purgeIfNecessary();
        }
        else
        {
            CacheLoaderConfig.IndividualCacheLoaderConfig first = getCacheLoaderConfig().getFirstCacheLoaderConfig();
            if (force ||
              (first != null && first.isPurgeOnStartup())) loader.remove(Fqn.ROOT);
        }
    }
}
