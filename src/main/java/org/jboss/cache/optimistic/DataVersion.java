/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.optimistic;

import java.io.Serializable;

/**
 * When versioning data nodes in optimistic locking, a DataVersion is assigned
 * to each node.  Versions need to implement the {@link #newerThan} method so
 * they can be compared during the validation phase upon commit.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public interface DataVersion extends Serializable
{
    /**
     * Returns true if this is a newer version than <code>other</code>.  There is no guarantee that the DataVersion
     * passed in will be of the same implementation, and as such, it is up to the implementation to perform
     * any type checking if needed before comparison.
     */
    public boolean newerThan(DataVersion other);
}
