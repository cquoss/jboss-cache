package org.jboss.cache;

import org.jgroups.View;

import java.util.Iterator;
import java.util.Set;

/**
 * This class provides a non-graphical view of <em>JBossCache</em> replication
 * events for a replicated cache.
 * <p>
 * It can be utilized as a standalone application or as a component in other
 * applications.
 * <p>
 * <strong>WARNING</strong>: take care when using this class in conjunction with
 * transactionally replicated cache's as it can cause deadlock situations due to
 * the reading of values for nodes in the cache.
 *
 * @author Jimmy Wilson 12-2004
 */
public class ConsoleListener extends AbstractTreeCacheListener
{
    private TreeCache _cache;
    private boolean   _startCache;

    /**
     * Constructor.
     * <p>
     * When using this constructor, this class with attempt to start and stop
     * the specified cache.
     *
     * @param cache the cache to monitor for replication events.
     */
    public ConsoleListener(TreeCache cache)
    throws Exception
    {
        this(cache, true, true);
    }

    /**
     * Constructor.
     *
     * @param cache the cache to monitor for replication events.
     *
     * @param startCache indicates whether or not the cache should be started by
     *                   this class.
     *
     * @param stopCache indicates whether or not the cache should be stopped by
     *                  this class.
     */
    public ConsoleListener(TreeCache cache,
                           boolean startCache, boolean stopCache)
    throws Exception
    {
        _cache = cache;
        _startCache = startCache;

        if (stopCache)
        {
            new ListenerShutdownHook().register();
        }
    }

    /**
     * Instructs this class to listen for cache replication events.
     * <p>
     * This method waits indefinately.  Use the notify method of this class
     * (using traditional Java thread notification semantics) to cause this
     * method to return.
     */
    public void listen()
    throws Exception
    {
        listen(true);
    }

    /**
     * Instructs this class to listen for cache replication events.
     *
     * @param wait whether or not this method should wait indefinately.
     *             <p>
     *             If this parameter is set to <code>true</code>, using the
     *             notify method of this class (using traditional Java thread
     *             notification semantics) will cause this method to return.
     */
    public void listen(boolean wait)
    throws Exception
    {
        _cache.addTreeCacheListener(this);

        if (_startCache)
        {
            _cache.startService();
        }

        if (wait)
        {
            synchronized(this)
            {
                wait();
            }
        }
    }

    /*
     * TreeCacheListener implementation.
     */

    public void cacheStarted(TreeCache cache)
    {
        printEvent("Cache started.");
    }

    public void cacheStopped(TreeCache cache)
    {
        printEvent("Cache stopped.");
    }

    public void nodeCreated(Fqn fqn)
    {
        printNode(fqn, "created");
    }

    public void nodeEvicted(Fqn fqn)
    {
        printEvent("DataNode evicted: " + fqn);
    }

    public void nodeLoaded(Fqn fqn)
    {
        printNode(fqn, "loaded");
    }
    
    public void nodeModified(Fqn fqn)
    {
        printNode(fqn, "modified");
    }
    
    public void nodeModify(Fqn fqn, boolean pre, boolean isLocal)
    {
       if(pre)
          printEvent("DataNode about to be modified: " + fqn);
       else
          printEvent("DataNode modified: " + fqn);
    }
    
    public void nodeRemoved(Fqn fqn)
    {
        printEvent("DataNode removed: " + fqn);
    }

    public void nodeVisited(Fqn fqn)
    {
        printEvent("DataNode visited: " + fqn);
    }

    public void viewChange(View new_view)
    {
        printEvent("View change: " + new_view);
    }
    
    public void nodeEvict(Fqn fqn, boolean pre)
    {
        if (pre) {
           printEvent("DataNode about to be evicted: " + fqn);
        } else {
           printEvent("DataNode evicted: " + fqn);
        }
    }
    
    public void nodeRemove(Fqn fqn, boolean pre, boolean isLocal)
    {
        if (pre) {
           printEvent("DataNode about to be removed: " + fqn);
        } else {
           printEvent("DataNode removed: " + fqn);
        }
    }
    
    public void nodeActivate(Fqn fqn, boolean pre)
    {
        if (pre) {
           printEvent("DataNode about to be activated: " + fqn);
        } else {
           printEvent("DataNode activated: " + fqn);
        }
    }
    
    public void nodePassivate(Fqn fqn, boolean pre)
    {
        if (pre) {
            printEvent("DataNode about to be evicted: " + fqn);
        } else {
            printEvent("DataNode evicted: " + fqn);
        }
    }

    /**
     * Prints an event message.
     *
     * @param eventSuffix the suffix of the event message.
     */
    private void printEvent(String eventSuffix)
    {
        System.out.print("EVENT");
        System.out.print(' ');

        System.out.println(eventSuffix);
    }

    /**
     * Prints the contents of the specified node.
     *
     * @param fqn the fully qualified name of the node to print.
     *
     * @param eventDetail a description of why the node is being printed.
     */
    private void printNode(Fqn fqn, String eventDetail)
    {
        System.out.print("EVENT");
        System.out.print(' ');
        System.out.print("DataNode");
        System.out.print(' ');
        System.out.print(eventDetail);
        System.out.print(':');
        System.out.print(' ');
        System.out.print(fqn);
        System.out.println();

        printNodeMap(fqn);

        System.out.println();
    }

    /**
     * Prints the contents of the specified node's <code>Map</code>.
     *
     * @param fqn the fully qualified name of the node containing the
     *            <code>Map</code> to be printed.
     */
    private void printNodeMap(Fqn fqn)
    {
        try
        {
            Set keys = _cache.getKeys(fqn);

            if (keys != null)
            {
                int maxKeyLength = 0;
                Iterator iterator = keys.iterator();

                while (iterator.hasNext())
                {
                    String key = (String) iterator.next();

                    int keyLength = key.length();

                    if (keyLength > maxKeyLength)
                    {
                        maxKeyLength = keyLength;
                    }
                }

                iterator = keys.iterator();

                while (iterator.hasNext())
                {
                    String key = (String) iterator.next();

                    System.out.print('\t');
                    System.out.print('[');

                    pad(key, maxKeyLength);

                    System.out.print(',');
                    System.out.print(' ');
                    System.out.print(_cache.get(fqn, key));
                    System.out.println(']');
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace(System.out);
        }
    }

    /**
     * Pads standard output for printing of the specified key.
     *
     * @param key the to be printed in a padded fashion.
     *
     * @param maxKeyLength the maximum length of the keys printed using this
     *                     method.  All keys whose length is less than this
     *                     value will be printed in padded fashion.
     */
    private void pad(String key, int maxKeyLength)
    {
        System.out.print(key);

        int keyLength = key.length();

        if (keyLength < maxKeyLength)
        {
            int padCount = maxKeyLength - keyLength;

            for (int i = 0; i < padCount; i++)
            {
                System.out.print(' ');
            }
        }
    }

    /**
     * This class provides a shutdown hook for shutting down the cache.
     */
    private class ListenerShutdownHook extends Thread
    {
        /**
         * Registers this hook for invocation during shutdown.
         */
        public void register()
        {
            Runtime.getRuntime().addShutdownHook(this);
        }

        /*
         * Thread overrides.
         */

        public void run()
        {
            _cache.stopService();
        }
    }

    /**
     * The main method.
     *
     * @param args command line arguments dictated by convention.
     *             <p>
     *             The first command line argument is the name of the
     *             <code>JBossCache</code> configuration file to be utilized
     *             for configuration of the cache.  Only the name of the
     *             configuration file is necessary as it is read off of the
     *             classpath.
     *             <p>
     *             If a configuration file is not specified on the command line,
     *             <code>jboss-cache.xml</code> will be the assumed file name.
     *             <p>
     *             All command line arguments after the first are ignored.
     */
    public static void main(String[] args)
    {
        final String DEFAULT_CONFIG_FILE_NAME = "jboss-cache.xml";

        try
        {
            TreeCache cache = new TreeCache();
            PropertyConfigurator configurator = new PropertyConfigurator();

            String configFileName = DEFAULT_CONFIG_FILE_NAME;

            if (args.length >= 1)
            {
                configFileName = args[0];
            } else {
                System.out.print("No xml config file argument is supplied. Will use jboss-cache.xml from classpath");
            }

            configurator.configure(cache, configFileName);

            ConsoleListener listener = new ConsoleListener(cache);
            listener.listen();
        }
        catch (Throwable throwable)
        {
            throwable.printStackTrace();
        }
    }
}
