/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.optimistic;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * The default implementation of a DataVersion, uses a <code>long</code> to
 * compare versions.
 *
 * This class is immutable.
 *
 * Also note that this is meant to control implicit, internal versioning.  Do not attempt to instantiate or use instances
 * of this class explicitly, via the {@link org.jboss.cache.config.Option#setDataVersion(DataVersion)} API, as it WILL
 * break things.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class DefaultDataVersion implements DataVersion, Externalizable
{
    private static final long serialVersionUID = -6896315742831861046L;
    /**
     * Version zero.
     * Assign this as the first version to your data.
     */
    public static final DataVersion ZERO = new DefaultDataVersion(0L);

    /**
     * Version one.
     */
    private static final DataVersion ONE = new DefaultDataVersion(1L);

    /**
     * Version two.
     */
    private static final DataVersion TWO = new DefaultDataVersion(2L);

    private long version;

    /**
     * Constructs with version 0.
     */
    public DefaultDataVersion()
    {
    }

    /**
     * Constructs with a version number.
     */
    public DefaultDataVersion(long version)
    {
        this.version = version;
    }

    /**
     * Returns a new DataVersion with a newer version number.
     */
    public DataVersion increment()
    {
        if (this == ZERO)
            return ONE;
        if (this == ONE)
            return TWO;
        return new DefaultDataVersion(version + 1);
    }

    public boolean newerThan(DataVersion other)
    {
        if (other instanceof DefaultDataVersion)
        {
            DefaultDataVersion dvOther = (DefaultDataVersion) other;
            return version > dvOther.version;
        }
        return false;
    }

    public String toString()
    {
        return "Ver="+version;
    }

    public boolean equals(Object other)
    {
        if (other instanceof DefaultDataVersion)
        {
            return version == ((DefaultDataVersion) other).version;
        }
        return false;
    }

    public int hashCode()
    {
        return (int)version;
    }

   public long getRawVersion()
   {
      return version;
   }

   public void writeExternal(ObjectOutput objectOutput) throws IOException
   {
      objectOutput.writeLong(version);
   }

   public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException
   {
      version = objectInput.readLong();
   }
}
