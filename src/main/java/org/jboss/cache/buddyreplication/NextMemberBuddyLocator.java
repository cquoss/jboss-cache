/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.buddyreplication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.stack.IpAddress;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This buddy locator uses a next-in-line algorithm to select buddies for a buddy group.  This algorithm
 * allows for the following properties, all of which are optional.
 * <p/>
 * <ul>
 * <li>More than one buddy per group - the <b>numBuddies</b> property, defaulting to 1 if ommitted.</li>
 * <li>Ability to skip buddies on the same host when selecting buddies - the <b>ignoreColocatedBuddies</b>
 * property, defaulting to true if ommitted.  Note that this is just a hint though, and if all nstances in
 * a cluster are colocated, the algorithm will be forced to pick a colocated instance even if this is property
 * set to true.</li>
 * </ul>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class NextMemberBuddyLocator implements BuddyLocator
{
    private Log log = LogFactory.getLog(NextMemberBuddyLocator.class);

    int numBuddies = 1;
    boolean ignoreColocatedBuddies = true;

    public void init(Properties props)
    {
        if (props != null)
        {
            String numBuddiesStr = props.getProperty("numBuddies");
            String ignoreColocatedBuddiesStr = props.getProperty("ignoreColocatedBuddies");
            if (numBuddiesStr != null) numBuddies = Integer.parseInt(numBuddiesStr);
            if (ignoreColocatedBuddiesStr != null)
            {
                ignoreColocatedBuddies = Boolean.valueOf(ignoreColocatedBuddiesStr).booleanValue();
            }
        }
    }

    public List locateBuddies(Map buddyPoolMap, List currentMembership, IpAddress dataOwner)
    {
        int numBuddiesToFind = Math.min(numBuddies, currentMembership.size());
        List buddies = new ArrayList(numBuddiesToFind);

        // find where we are in the list.
        int dataOwnerSubscript = currentMembership.indexOf(dataOwner);
        int i = 0;
        boolean ignoreColocatedBuddiesForSession = ignoreColocatedBuddies;


        while (buddies.size() < numBuddiesToFind)
        {
            int subscript = i + dataOwnerSubscript + 1;
            // make sure we loop around the list
            if (subscript >= currentMembership.size()) subscript = subscript - currentMembership.size();

            // now if subscript is STILL greater than or equal to the current membership size, we've looped around
            // completely and still havent found any more suitable candidates.  Try with colocation hint disabled.
            if (subscript >= currentMembership.size() && ignoreColocatedBuddiesForSession)
            {
                ignoreColocatedBuddiesForSession = false;
                i = 0;
                if (log.isInfoEnabled())
                {
                    log.info("Expected to look for " + numBuddiesToFind + " buddies but could only find " + buddies.size() + " suitable candidates - trying with colocated buddies as well.");
                }
                continue;
            }

            // now try disabling the buddy pool
            if (subscript >= currentMembership.size() && buddyPoolMap != null)
            {
                buddyPoolMap = null;
                ignoreColocatedBuddiesForSession = ignoreColocatedBuddies; // reset this flag
                i = 0;
                if (log.isInfoEnabled())
                {
                    log.info("Expected to look for " + numBuddiesToFind + " buddies but could only find " + buddies.size() + " suitable candidates - trying again, ignoring buddy pool hints.");
                }
                continue;
            }

            // now if subscript is STILL greater than or equal to the current membership size, we've looped around
            // completely and still havent found any more suitable candidates.  Give up with however many we have.
            if (subscript >= currentMembership.size())
            {
                if (log.isInfoEnabled())
                {
                    log.info("Expected to look for " + numBuddiesToFind + " buddies but could only find " + buddies.size() + " suitable candidates!");
                }
                break;
            }

            IpAddress candidate = (IpAddress) currentMembership.get(subscript);
            if (
                    !candidate.equals(dataOwner) && // ignore self from selection as buddy
                            !buddies.contains(candidate) && // havent already considered this candidate
                            (!ignoreColocatedBuddiesForSession || !isColocated(candidate, dataOwner)) && // ignore colocated buddies
                            (isInSameBuddyPool(buddyPoolMap, candidate, dataOwner)) // try and find buddies in the same buddy pool first
                    )
            {
                buddies.add(candidate);
            }
            i++;
        }

        if (log.isTraceEnabled()) log.trace("Selected buddy group as " + buddies);
        return buddies;
    }

    private boolean isInSameBuddyPool(Map buddyPoolMap, Object candidate, Object dataOwner)
    {
        if (buddyPoolMap == null) return true;
        Object ownerPoolName = buddyPoolMap.get(dataOwner);
        Object candidatePoolName = buddyPoolMap.get(candidate);
        if (ownerPoolName == null || candidatePoolName == null) return false;
        return ownerPoolName.equals(candidatePoolName);
    }

    private boolean isColocated(IpAddress candidate, IpAddress dataOwner)
    {
        // assume they're both IpAddresses??
        InetAddress inetC = candidate.getIpAddress();
        InetAddress inetD = dataOwner.getIpAddress();

        if (inetC.equals(inetD)) return true;

        // now check other interfaces.
        try
        {
            for (Enumeration nics = NetworkInterface.getNetworkInterfaces(); nics.hasMoreElements();)
            {
                NetworkInterface i = (NetworkInterface) nics.nextElement();
                for (Enumeration addrs = i.getInetAddresses(); addrs.hasMoreElements();)
                {
                    InetAddress addr = (InetAddress) addrs.nextElement();
                    if (addr.equals(inetC)) return true;
                }
            }
        }
        catch (SocketException e)
        {
            if (log.isDebugEnabled()) log.debug("Unable to read NICs on host", e);
            if (log.isInfoEnabled())
            {
                log.info("UNable to read all network interfaces on host " + inetD + " to determine colocation of " + inetC + ".  Assuming " + inetC + " is NOT colocated with " + inetD);
            }
        }

        return false;
    }
}