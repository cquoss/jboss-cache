/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.marshall;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.buddyreplication.BuddyManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * Factory to create region from configuration, to track region,
 * and to resolve naming conflict for regions. Note that in addition to
 * user-specified regions, there is also a global cache <code>_default_</code>
 * region that covers everything else.
 * <p/>
 * <p>Note that this is almost identical to the one used in eviction policy. We will
 * need to refactor them in the future for everyone to use.</p>
 *
 * @author Ben Wang 08-2005
 * @version $Id: RegionManager.java 3641 2007-03-06 20:09:56Z msurtani $
 */
public class RegionManager
{

    private Log log_ = LogFactory.getLog(RegionManager.class);

    private final Map regionMap_ = new ConcurrentHashMap();
   private int longestFqn = 0;


    public RegionManager()
    {
    }

    /**
     * Create a region based on fqn.
     *
     * @param fqn The region identifier.
     * @param cl  Class loader
     * @throws RegionNameConflictException
     */
    public Region createRegion(String fqn, ClassLoader cl)
            throws RegionNameConflictException
    {
        return createRegion(fqn, cl, false);
    }

    /**
     * Create a region based on fqn.
     *
     * @param fqn The region identifier.
     * @param cl  Class loader
     * @throws RegionNameConflictException
     */
    public Region createRegion(String fqn, ClassLoader cl, boolean inactive)
            throws RegionNameConflictException
    {
        return createRegion(Fqn.fromString(fqn), cl, inactive);
    }

    /**
     * Create a region based on fqn.
     *
     * @param fqn The region identifier.
     * @param cl  Class loader
     * @throws RegionNameConflictException
     */
    public Region createRegion(Fqn fqn, ClassLoader cl, boolean inactive)
            throws RegionNameConflictException
    {
        if (log_.isDebugEnabled())
        {
            log_.debug("createRegion(): creating region for fqn- " + fqn);
        }

        checkConflict(fqn);
        Region region = new Region(fqn.toString() + Fqn.SEPARATOR, cl, inactive);
        regionMap_.put(fqn, region);
        longestFqn = Math.max(longestFqn, fqn.size());
        return region;
    }

    /**
     * Removes a region by string.
     */
    public void removeRegion(String fqn)
    {
        removeRegion(Fqn.fromString(fqn));
    }

    /**
     * Removes a region by Fqn, returns true if the region was found.
     */
    public boolean removeRegion(Fqn fqn)
    {
        Region region = (Region) regionMap_.remove(fqn);
        return region != null;
    }

    /**
     * Returns true if the region exists.
     */
    public boolean hasRegion(String myFqn)
    {
        return hasRegion(Fqn.fromString(myFqn));
    }

    /**
     * Returns true if the region exists.
     */
    public boolean hasRegion(Fqn fqn)
    {
        return regionMap_.containsKey(fqn);
    }

    /**
     * Returns the Region belonging to a String FQN.
     */
    public Region getRegion(String myFqn)
    {
        return getRegion(Fqn.fromString(myFqn));
    }

    /**
     * Returns the Region corresponding to this Fqn.
     */
    public Region getRegion(Fqn fqn)
    {
        Fqn orig = fqn;
        // Truncate Fqn as an optimization
        if (fqn.size() > longestFqn)
        {
            fqn = fqn.getFqnChild(0, longestFqn);
        }

        Region region;
        do
        {
            region = (Region) regionMap_.get(fqn);
            if (region != null)
            {
                return region;
            }
            fqn = fqn.getParent();
        }
        while (!fqn.isRoot());

        if (log_.isTraceEnabled())
        {
            log_.trace("getRegion(): user-specified region not found: " + orig);
        }
        return null;
    }

    /**
     * Gets the {@link Region}s managed by this object.
     *
     * @return the regions. Will not return <code>null</code>.
     */
    public Region[] getRegions()
    {
        Set s = new TreeSet(new RegionComparator());
        for (Iterator i = regionMap_.values().iterator(); i.hasNext();) s.add(i.next());

        return (Region[]) s.toArray(new Region[]{});
    }

    /**
     * Check for conflict in the current regions. There is a conflict
     * if fqn is any parent fqn of the current regions.
     *
     * @param myFqn Current fqn for potential new region.
     * @throws RegionNameConflictException to indicate a region name conflict has ocurred.
     */
    public void checkConflict(String myFqn) throws RegionNameConflictException
    {
        checkConflict(Fqn.fromString(myFqn));
    }

    /**
     * Check for conflict in the current regions. There is a conflict
     * if fqn is any parent fqn of the current regions.
     */
    public void checkConflict(Fqn fqn) throws RegionNameConflictException
    {
        for (int i = 0; i < fqn.size(); i++)
        {
            Fqn child = fqn.getFqnChild(i);
            if (regionMap_.containsKey(child))
            {
                throw new RegionNameConflictException("RegionManager.checkConflict(): new region fqn "
                        + fqn + " is in conflict with existing region fqn " + child);
            }
        }
        // We are clear then.
    }

    /**
     * Helper utility that checks for a classloader registered for the
     * given Fqn, and if found sets it as the TCCL. If the given Fqn is
     * under the _BUDDY_BACKUP_ region, the equivalent region in the main
     * tree is used to find the classloader.
     *
     * @param subtree Fqn pointing to a region for which a special classloader
     *                may have been registered.
     */
    public void setUnmarshallingClassLoader(Fqn subtree)
    {
        if (subtree.isChildOf(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN))
        {
            if (subtree.size() <= 2)
            {
                subtree = Fqn.ROOT;
            }
            else
            {
                subtree = subtree.getFqnChild(2, subtree.size());
            }
        }
        Region region = getRegion(subtree);
        ClassLoader regionCL = (region == null) ? null : region.getClassLoader();
        if (regionCL != null)
        {
            Thread.currentThread().setContextClassLoader(regionCL);
        }
    }

    static class RegionComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            Region r1 = (Region) o1;
            Region r2 = (Region) o2;
            String f1 = r1.getFqn();
            String f2 = r2.getFqn();
            return f1.compareTo(f2);
        }
    }

}
