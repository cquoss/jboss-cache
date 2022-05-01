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
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A cache loader that consults other members in the cluster for values.  Does
 * not propagate update methods since replication should take care of this.  A
 * <code>timeout</code> property is required, a <code>long</code> that
 * specifies in milliseconds how long to wait for results before returning a
 * null.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class ClusteredCacheLoader implements CacheLoader
{
    protected long timeout = 10000;
    protected TreeCache cache;
    private static Log log = LogFactory.getLog(ClusteredCacheLoader.class);

    /**
     * Sets the configuration.
     * A property <code>timeout</code> is used as the timeout value.
     *
     * @param props properties to use
     */
    public void setConfig(Properties props)
    {
        try
        {
            timeout = Long.valueOf(props.getProperty("timeout")).longValue();
        }
        catch (Exception e)
        {
            log.info("Using default value for config property 'timeout' - " + timeout);
        }
    }

    public void setCache(TreeCache c)
    {
        this.cache = c;
    }

    public Set getChildrenNames(Fqn fqn) throws Exception
    {
        MethodCall call = MethodCallFactory.create(MethodDeclarations.getChildrenNamesMethodLocal, new Object[]{fqn});
        Object resp = callRemote(call);
        return (Set) resp;
    }

    private Object callRemote(MethodCall call) throws Exception
    {
        if (log.isTraceEnabled()) log.trace("cache=" + cache.getLocalAddress() + "; calling with " + call);
        Vector mbrs = cache.getMembers();
        MethodCall clusteredGet = MethodCallFactory.create(MethodDeclarations.clusteredGetMethod, new Object[]{call, Boolean.FALSE});
        List resps = cache.callRemoteMethods(mbrs, clusteredGet, GroupRequest.GET_FIRST, true, timeout);
        if (resps == null)
        {
            if (log.isInfoEnabled()) log.info("No replies to call " + call + ".  Perhaps we're alone in the cluster?");
            return null;
        }
        else
        {
            // test for and remove exceptions
            Iterator i = resps.iterator();
            Object result = null;
            while(i.hasNext())
            {
                Object o = i.next();
                if (o instanceof Exception)
                {
                    if (log.isDebugEnabled()) log.debug("Found remote exception among responses - removing from responses list", (Exception)o);
                }
                else
                {
                    // keep looping till we find a FOUND answer.
                    List clusteredGetResp = (List) o;
                    // found?
                    if (((Boolean) clusteredGetResp.get(0)).booleanValue())
                    {
                        result = clusteredGetResp.get(1);
                        break;
                    }
                }
            }

            if (log.isTraceEnabled()) log.trace("got responses " + resps);
            return result;
        }
    }

    public Map get(Fqn name) throws Exception
    {
        return get0(name);
    }

    protected Map get0(Fqn name) throws Exception
    {
        MethodCall call = MethodCallFactory.create(MethodDeclarations.getDataMapMethodLocal, new Object[]{name});
        Object resp = callRemote(call);
        Map m = (Map)resp;
        if (m != null)
        {
           // This eliminates a problem of seeing uninitialized nodes
           m.remove(TreeCache.UNINITIALIZED);
        }
        return m;
    }

    public boolean exists(Fqn name) throws Exception
    {
        MethodCall call = MethodCallFactory.create(MethodDeclarations.existsMethod, new Object[]{name});
        Object resp = callRemote(call);

        return resp != null && ((Boolean) resp).booleanValue();
    }

    public Object put(Fqn name, Object key, Object value) throws Exception
    {
      if (cache.getInvocationContext().isOriginLocal())
      {
        Object o[] = { name, key, Boolean.TRUE };
        MethodCall call = MethodCallFactory.create(MethodDeclarations.getKeyValueMethodLocal, o);
        return callRemote(call);
      }
      else
      {
         log.trace("Call originated remotely.  Not bothering to try and do a clustered get() for this put().  Returning null.");
         return null;
      }
    }

    /**
     * Does nothing; replication handles put.
     */
    public void put(Fqn name, Map attributes) throws Exception
    {
    }

    /**
     * Does nothing; replication handles put.
     */
    public void put(List modifications) throws Exception
    {
    }

    /**
     * Fetches the remove value, does not remove.  Replication handles
     * removal.
     */
    public Object remove(Fqn name, Object key) throws Exception
    {
        Map map = get(name);
        return map == null ? null : map.get(key);
    }

    /**
     * Does nothing; replication handles removal.
     */
    public void remove(Fqn name) throws Exception
    {
        // do nothing
    }

    /**
     * Does nothing; replication handles removal.
     */
    public void removeData(Fqn name) throws Exception
    {
    }

    /**
     * Does nothing.
     */
    public void prepare(Object tx, List modifications, boolean one_phase) throws Exception
    {
    }

    /**
     * Does nothing.
     */
    public void commit(Object tx) throws Exception
    {
    }

    /**
     * Does nothing.
     */
    public void rollback(Object tx)
    {
    }

    /**
     * Returns an empty byte array.
     */
    public byte[] loadEntireState() throws Exception
    {
        return new byte[0];
    }

    /**
     * Does nothing.
     */
    public void storeEntireState(byte[] state) throws Exception
    {
    }

    public void create() throws Exception
    {
    }

    public void start() throws Exception
    {
    }

    public void stop()
    {
    }

    public void destroy()
    {
    }

}
