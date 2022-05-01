/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.View;

/**
 * An abstract implementation of {@link TreeCacheListener} and {@link ExtendedTreeCacheListener}.  Subclass this if you
 * don't want to implement all the methods in the {@link TreeCacheListener} and {@link ExtendedTreeCacheListener}
 * interfaces.
 *
 * @author hmesha
 */
public abstract class AbstractTreeCacheListener implements TreeCacheListener, ExtendedTreeCacheListener
{
    
    private static Log log=LogFactory.getLog(AbstractTreeCacheListener.class);

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#nodeCreated(org.jboss.cache.Fqn)
     */
    public void nodeCreated(Fqn fqn)
    {
        if(log.isTraceEnabled())
            log.trace("Event DataNode created: " + fqn);

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#nodeRemoved(org.jboss.cache.Fqn)
     */
    public void nodeRemoved(Fqn fqn)
    {
        if(log.isTraceEnabled())
            log.trace("Event DataNode removed: " + fqn);

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#nodeLoaded(org.jboss.cache.Fqn)
     */
    public void nodeLoaded(Fqn fqn)
    {
        if(log.isTraceEnabled())
            log.trace("Event DataNode loaded: " + fqn);

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#nodeEvicted(org.jboss.cache.Fqn)
     */
    public void nodeEvicted(Fqn fqn)
    {
        if(log.isTraceEnabled())
            log.trace("Event DataNode evicted: " + fqn);

    }


    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#nodeModify(org.jboss.cache.Fqn, boolean pre)
     */
    public void nodeModify(Fqn fqn, boolean pre, boolean isLocal)
    {
        if(log.isTraceEnabled()){
            if(pre)
                log.trace("Event DataNode about to be modified: " + fqn);
            else
                log.trace("Event DataNode modified: " + fqn);
        }
    }
    
    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#nodeModified(org.jboss.cache.Fqn)
     */
    public void nodeModified(Fqn fqn)
    {
        if(log.isTraceEnabled())
            log.trace("Event DataNode modified: " + fqn);

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#nodeVisited(org.jboss.cache.Fqn)
     */
    public void nodeVisited(Fqn fqn)
    {
        if(log.isTraceEnabled())
            log.trace("Event DataNode visited: " + fqn);

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#cacheStarted(org.jboss.cache.TreeCache)
     */
    public void cacheStarted(TreeCache cache)
    {
        if(log.isTraceEnabled())
            log.trace("Event Cache started");

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#cacheStopped(org.jboss.cache.TreeCache)
     */
    public void cacheStopped(TreeCache cache)
    {
        if(log.isTraceEnabled())
            log.trace("Event Cache stopped");

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.TreeCacheListener#viewChange(org.jgroups.View)
     */
    public void viewChange(View new_view)
    {
        if(log.isTraceEnabled())
            log.trace("Event View change: " + new_view);

    }

    /* (non-Javadoc)
     * @see org.jboss.cache.ExtendedTreeCacheListener#nodeEvict(org.jboss.cache.Fqn, boolean)
     */
    public void nodeEvict(Fqn fqn, boolean pre)
    {
        if(log.isTraceEnabled()) {
            if (pre) {
                log.trace("Event DataNode about to be evicted: " + fqn);
            } else {
                log.trace("Event DataNode evicted: " + fqn);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.jboss.cache.ExtendedTreeCacheListener#nodeRemove(org.jboss.cache.Fqn, boolean)
     */
    public void nodeRemove(Fqn fqn, boolean pre, boolean isLocal)
    {
        if(log.isTraceEnabled()) {
            if (pre) {
                log.trace("Event DataNode about to be removed: " + fqn);
            } else {
                log.trace("Event DataNode removed: " + fqn);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.jboss.cache.ExtendedTreeCacheListener#nodeActivate(org.jboss.cache.Fqn, boolean)
     */
    public void nodeActivate(Fqn fqn, boolean pre)
    {
        if(log.isTraceEnabled()) {
            if (pre) {
                log.trace("Event DataNode about to be activated: " + fqn);
            } else {
                log.trace("Event DataNode activated: " + fqn);
            }
         }
    }

    /* (non-Javadoc)
     * @see org.jboss.cache.ExtendedTreeCacheListener#nodePassivate(org.jboss.cache.Fqn, boolean)
     */
    public void nodePassivate(Fqn fqn, boolean pre)
    {
        if(log.isTraceEnabled()) {
            if (pre) {
                log.trace("Event DataNode about to be passivated: " + fqn);
            } else {
                log.trace("Event DataNode passivated: " + fqn);
            }
         }
    }
}
