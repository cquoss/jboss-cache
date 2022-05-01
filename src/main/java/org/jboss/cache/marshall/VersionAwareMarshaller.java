/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.marshall;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.invocation.MarshalledValueOutputStream;
import org.jboss.invocation.MarshalledValueInputStream;
import org.jgroups.blocks.RpcDispatcher;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * A facade to various other marshallers like {@link LegacyTreeCacheMarshaller} and {@link TreeCacheMarshaller140}
 * which is version-aware.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class VersionAwareMarshaller implements RpcDispatcher.Marshaller
{
    private RegionManager manager;
    private boolean defaultInactive, useRegionBasedMarshalling;
    private Log log = LogFactory.getLog(VersionAwareMarshaller.class);

    Marshaller defaultMarshaller;
    Map marshallers = new HashMap();

    private static final int VERSION_LEGACY = 1;
    private static final int VERSION_140 = 14;
    private static final int VERSION_200 =20;

    private int versionInt;


    public VersionAwareMarshaller(RegionManager manager, boolean defaultInactive, boolean useRegionBasedMarshalling, String version)
    {
        this.manager = manager;
        this.defaultInactive = defaultInactive;
        this.useRegionBasedMarshalling = useRegionBasedMarshalling;

        // convert the replication version passed in to the MINOR version.
        // E.g., 1.4.1.SP3 -> 1.4.0

        versionInt = toMinorVersionInt(version);

        switch (versionInt)
        {
            case VERSION_200:
            case VERSION_140:
                defaultMarshaller = new TreeCacheMarshaller140(manager, defaultInactive, useRegionBasedMarshalling);
                marshallers.put(new Integer(VERSION_140), defaultMarshaller);
                break;
            default:
                defaultMarshaller = new LegacyTreeCacheMarshaller(manager, defaultInactive, useRegionBasedMarshalling);
                marshallers.put(new Integer(VERSION_LEGACY), defaultMarshaller);
        }

        if (log.isDebugEnabled())
        {
            log.debug("Initialised with version "+version+" and versionInt " + versionInt);
            log.debug("Using default marshaller " + defaultMarshaller.getClass());
        }
    }

    /**
     * Converts versions to known compatible version ids.
     * <p/>
     * Typical return values:
     * <p/>
     * < 1.4.0 = "1"
     * 1.4.x = "14"
     * 1.5.x = "15"
     * 2.0.x = "20"
     * 2.1.x = "21"
     * <p/>
     * etc.
     *
     * @param version
     * @return a version integer
     */
    private int toMinorVersionInt(String version)
    {
        try
        {
            StringTokenizer strtok = new StringTokenizer(version, ".");

            // major, minor, micro, patch
            String[] versionComponents = {null, null, null, null};
            int i = 0;
            while (strtok.hasMoreTokens())
            {
                versionComponents[i++] = strtok.nextToken();
            }

            int major = Integer.parseInt(versionComponents[0]);
            int minor = Integer.parseInt(versionComponents[1]);

            return (major > 1 || minor > 3) ? (10 * major) + minor : 1;
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Unsupported replication version string " + version);
        }
    }

    public byte[] objectToByteBuffer(Object obj) throws Exception
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out;

        // based on the default marshaller, construct an object output stream based on what's compatible.
        if (defaultMarshaller instanceof LegacyTreeCacheMarshaller)
        {
            out = new MarshalledValueOutputStream(bos);
        }
        else
        {
            out = ObjectSerializationFactory.createObjectOutputStream(bos);
            out.writeShort(versionInt);
        }

       //now marshall the contents of the object
        defaultMarshaller.objectToStream(obj, out);
        out.close();

        // and return bytes.
        return bos.toByteArray();
    }

    public Object objectFromByteBuffer(byte[] buf) throws Exception
    {
        Marshaller marshaller;
        int versionId = VERSION_LEGACY;
        ObjectInputStream in = null;
        try
        {
            // just a peek - does not actually "remove" these bytes from the stream.
            // create an input stream and read the first short
            in = ObjectSerializationFactory.createObjectInputStream(buf);
            versionId = in.readShort();
        }
        catch (Exception e)
        {
            log.debug("Unable to read version id from first two bytes of stream.  Probably from a version prior to JBC 1.4.0.  Reverting to using a LegacyTreeCacheMarshaller for this communication.");
        }

        marshaller = getMarshaller(versionId);

        if (marshaller instanceof LegacyTreeCacheMarshaller)
        {
            // create a more 'compatible' ObjectInputStream.
            in = new MarshalledValueInputStream(new ByteArrayInputStream(buf));
        }

        try
        {
            return marshaller.objectFromStream(in);
        }
        catch (Exception e)
        {
            if (marshaller instanceof LegacyTreeCacheMarshaller)
            {
                // the default marshaller IS the legacy marshaller!!
                throw e;
            }
            else
            {
                // retry with legacy marshaller - this is probably because the version id was incorrectly read.
                if (log.isInfoEnabled())
                {
                    log.info("Caught exception unmarshalling stream with specific versioned marshaller " + marshaller.getClass() + ".  Attempting to try again with legacy marshaller " + LegacyTreeCacheMarshaller.class);
                }
                // create a more 'compatible' ObjectInputStream.
                try
                {
                   in = new MarshalledValueInputStream(new ByteArrayInputStream(buf));
                   return getMarshaller(VERSION_LEGACY).objectFromStream(in);
                }
                catch (Exception e1)
                {
                   log.debug("Retry with legacy marshaller failed as well; throwing original exception", e1);
                   throw e;
                }
            }
        }
    }

    /**
     * Lazily instantiates and loads the relevant marshaller for a given version.
     *
     * @param versionId
     * @return appropriate marshaller for the version requested.
     */
    Marshaller getMarshaller(int versionId)
    {
        Marshaller marshaller;
        switch (versionId)
        {
            case VERSION_140:
                marshaller = (Marshaller) marshallers.get(new Integer(VERSION_140));
                if (marshaller == null)
                {
                    marshaller = new TreeCacheMarshaller140(manager, defaultInactive, useRegionBasedMarshalling);
                    marshallers.put(new Integer(VERSION_140), marshaller);
                }
                break;
            default:
                marshaller = (Marshaller) marshallers.get(new Integer(VERSION_LEGACY));
                if (marshaller == null)
                {
                    marshaller = new LegacyTreeCacheMarshaller(manager, defaultInactive, useRegionBasedMarshalling);
                    marshallers.put(new Integer(VERSION_LEGACY), marshaller);
                }
        }
        return marshaller;
    }


    public void registerClassLoader(String fqn, ClassLoader cl) throws RegionNameConflictException
    {
        defaultMarshaller.registerClassLoader(fqn, cl);
    }

    public void unregisterClassLoader(String fqn) throws RegionNotFoundException
    {
        defaultMarshaller.unregisterClassLoader(fqn);
    }

    public boolean isInactive(String s)
    {
        return defaultMarshaller.isInactive(s);
    }

    public ClassLoader getClassLoader(String fqnS) throws RegionNotFoundException
    {
        return defaultMarshaller.getClassLoader(fqnS);
    }

    public void inactivate(String subtreeFqn) throws RegionNameConflictException
    {
        defaultMarshaller.inactivate(subtreeFqn);
    }

    public void activate(String subtreeFqn) throws RegionNameConflictException
    {
        defaultMarshaller.activate(subtreeFqn);
    }
}
