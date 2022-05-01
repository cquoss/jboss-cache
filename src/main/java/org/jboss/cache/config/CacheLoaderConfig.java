/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.config;

import org.jboss.cache.xml.XmlHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.io.ByteArrayInputStream;

/**
 * Holds the configuration of the cache loader chain.  ALL cache loaders should be defined using this class, adding
 * individual cache loaders to the chain by calling {@see CacheLoaderConfig#addIndividualCacheLoaderConfig}
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class CacheLoaderConfig
{
    private boolean passivation;
    private String preload;
    private List cacheLoaderConfigs = new ArrayList();

    private boolean shared;

    public String getPreload()
    {
        return preload;
    }

    public void setPreload(String preload)
    {
        this.preload = preload;
    }

    public void setPassivation(boolean passivation)
    {
        this.passivation = passivation;
    }

    public boolean isPassivation()
    {
        return passivation;
    }

    public void addIndividualCacheLoaderConfig(IndividualCacheLoaderConfig clc)
    {
        cacheLoaderConfigs.add(clc);
    }

    public List getIndividualCacheLoaderConfigs()
    {
        return cacheLoaderConfigs;
    }

    public IndividualCacheLoaderConfig getFirstCacheLoaderConfig()
    {
        if (cacheLoaderConfigs.size() == 0) return null;
        return (IndividualCacheLoaderConfig) cacheLoaderConfigs.get(0);
    }

    public boolean useChainingCacheLoader()
    {
        return !isPassivation() && cacheLoaderConfigs.size() > 1;
    }

    public String toString()
    {
        return new StringBuffer().append("CacheLoaderConfig{").append("shared=").append(shared).append(", passivation=").append(passivation).append(", preload='").append(preload).append('\'').append(", cacheLoaderConfigs.size()=").append(cacheLoaderConfigs.size()).append('}').toString();
    }

    public void setShared(boolean shared)
    {
        this.shared = shared;
    }

    public boolean isShared()
    {
        return shared;
    }


    /**
     * Configuration object that holds the confguration of an individual cache loader.
     *
     * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
     */
    public static class IndividualCacheLoaderConfig
    {
        private String className;
        private boolean async;
        private boolean ignoreModifications;
        private boolean fetchPersistentState;

        public boolean isPurgeOnStartup()
        {
            return purgeOnStartup;
        }

        private boolean purgeOnStartup;

        public boolean isFetchPersistentState()
        {
            return fetchPersistentState;
        }

        public void setFetchPersistentState(boolean fetchPersistentState)
        {
            this.fetchPersistentState = fetchPersistentState;
        }

        private Properties properties;

        public void setClassName(String className)
        {
            this.className = className;
        }

        public String getClassName()
        {
            return className;
        }

        public void setAsync(boolean async)
        {
            this.async = async;
        }

        public boolean isAsync()
        {
            return async;
        }

        public void setIgnoreModifications(boolean ignoreModifications)
        {
            this.ignoreModifications = ignoreModifications;
        }

        public boolean isIgnoreModifications()
        {
            return ignoreModifications;
        }

       public void setProperties(String properties) throws IOException
        {
            if (properties == null) return;
            // JBCACHE-531: escape all backslash characters
            // replace any "\" that is not preceded by a backslash with "\\"
            properties = XmlHelper.escapeBackslashes(properties);
            ByteArrayInputStream is = new ByteArrayInputStream(properties.trim().getBytes("ISO8859_1"));
            this.properties = new Properties();
            this.properties.load(is);
            is.close();
        }

        public void setProperties(Properties properties)
        {
            this.properties = properties;
        }

        public Properties getProperties()
        {
            return properties;
        }


        public String toString()
        {
            return new StringBuffer().append("IndividualCacheLoaderConfig{").append("className='").append(className).append('\'').append(", async=").append(async).append(", ignoreModifications=").append(ignoreModifications).append(", fetchPersistentState=").append(fetchPersistentState).append(", properties=").append(properties).append('}').toString();
        }

        public void setPurgeOnStartup(boolean purgeOnStartup)
        {
            this.purgeOnStartup = purgeOnStartup;
        }
    }
}
