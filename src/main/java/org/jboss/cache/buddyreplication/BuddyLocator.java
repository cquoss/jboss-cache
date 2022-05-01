/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.buddyreplication;

import org.jgroups.stack.IpAddress;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Buddy Locators help the {@see BuddyManager} select buddies for its buddy group.
 * <p>
 * Implementations of this class must declare a public no-arguments constructor.
 * </p>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public interface BuddyLocator
{
   /**
    * Initialize this <code>BuddyLocator</code>.
    * 
    * @param props  an implementation-specific set of configuration properties.
    *               May be <code>null</code>.
    */
    public void init(Properties props);

    /**
     * Choose a set of buddies for the given node.  Invoked when a change in
     * cluster membership is detected.
     * 
     * @param buddyPoolMap  Map<IpAddress, String> mapping nodes in the cluster to
     *                      the "buddy pool" they have identified themselves as 
     *                      belonging too.  A BuddyLocator implementation can use
     *                      this information to preferentially assign buddies from
     *                      the same buddy pool as <code>dataOwner</code>.  May be
     *                      <code>null</code> if buddy pools aren't configured.
     * @param currentMembership List<IpAddress> of the current cluster members
     * @param dataOwner IpAddress of the node for which buddies should be selected
     * 
     * @return List<IpAddress> of the nodes that should serve as buddies for
     *         <code>dataOwner</code>. Will not be <code>null</code>, may
     *         be empty.
     */
    public List locateBuddies(Map buddyPoolMap, List currentMembership, IpAddress dataOwner);
}
