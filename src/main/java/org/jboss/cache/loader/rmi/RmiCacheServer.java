package org.jboss.cache.loader.rmi;

import org.jboss.cache.PropertyConfigurator;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeCacheMBean;
import org.jboss.system.ServiceMBeanSupport;
import org.jboss.mx.util.MBeanProxyExt;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.rmi.Naming;

/**
 * Server which exports an RMI stub to the TreeCache. Clients can use the RmiDelegatingCacheLoader to remotely
 * delegate to this TreeCache
 *
 * @author Bela Ban
 * @version $Id: RmiCacheServer.java 1121 2006-02-03 16:37:10Z msurtani $
 */
public class RmiCacheServer extends ServiceMBeanSupport implements RmiCacheServerMBean
{
    TreeCacheMBean cache;
    RemoteTreeCacheImpl remoteObj;
    String bindAddress;
    String mbeanServerName;
    int port;
    String configFile;
    String bindName;
    ObjectName cacheName;

    public String getBindAddress()
    {
        return bindAddress;
    }

    public void setBindAddress(String bindAddress)
    {
        this.bindAddress = bindAddress;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getMBeanServerName()
    {
        return mbeanServerName;
    }

    public void setMBeanServerName(String name)
    {
        mbeanServerName = name;
    }

    public String getConfig()
    {
        return configFile;
    }

    public void setConfig(String config)
    {
        configFile = config;
    }

    public TreeCacheMBean getCache()
    {
        return cache;
    }

    public void setCache(TreeCacheMBean cache)
    {
        this.cache = cache;
    }

    public String getCacheName()
    {
        return cacheName != null ? cacheName.toString() : "n/a";
    }

    public void setCacheName(String cacheName) throws MalformedObjectNameException
    {
        this.cacheName = new ObjectName(cacheName);
    }

    public String getBindName()
    {
        return bindName;
    }

    public void setBindName(String bindName)
    {
        this.bindName = bindName;
    }

    public RmiCacheServer(String host, int port, String bindName, String config)
    {
        this.bindAddress = host;
        this.port = port;
        this.configFile = config;
        this.bindName = bindName;
    }

    public void createService() throws Exception
    {
        super.createService();
    }

    public void startService() throws Exception
    {
        if (cache == null)
        {
            // 1. check whether we have an object name, pointing to the cache MBean
            if (cacheName != null && server != null)
            {
                cache = (TreeCacheMBean) MBeanProxyExt.create(TreeCacheMBean.class, cacheName, server);
            }
        }

        if (cache == null)
        {
            cache = new TreeCache();
            PropertyConfigurator config = new PropertyConfigurator();
            config.configure(cache, configFile);
            cache.startService(); // kick start tree cache
        }

        remoteObj = new RemoteTreeCacheImpl(cache);
        Naming.rebind("//" + bindAddress + ":" + port + "/" + bindName, remoteObj);
    }

    public void stopService()
    {
        if (cache != null)
        {
            cache.stopService();
            cache.destroyService();
            cache = null;
        }
        if (remoteObj != null)
        {
            try
            {
                Naming.unbind("//" + bindAddress + ":" + port + "/" + bindName);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public void destroyService() throws Exception
    {
        super.destroyService();
    }


    public static void main(String[] args)
    {
        String bindAddress = "localhost", configFile = "cache-service.xml", bindName = "rmiCacheLoader";
        int port = 1098;
        RmiCacheServer server;

        for (int i = 0; i < args.length; i++)
        {
            if (args[i].equals("-bindAddress"))
            {
                bindAddress = args[++i];
                continue;
            }
            if (args[i].equals("-port"))
            {
                port = Integer.parseInt(args[+i]);
                continue;
            }
            if (args[i].equals("-config"))
            {
                configFile = args[++i];
                continue;
            }
            if (args[i].equals("-bindName"))
            {
                bindName = args[++i];
                continue;
            }
            help();
            return;
        }
        server = new RmiCacheServer(bindAddress, port, bindName, configFile);
        try
        {
            server.start();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    private static void help()
    {
        System.out.println("CacheServer [-bindAddress <host>] [-port <port>] [-bindName <RMI bind name>] [-config <cache config>] [-help]");
    }
}
