/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.marshall;

import org.jboss.invocation.MarshalledValueInputStream;
import org.jboss.invocation.MarshalledValueOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Utility methods related to marshalling and unmarshalling objects.
 *
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Revision$
 */
public class MarshallUtil
{

    /**
     * Creates an object from a byte buffer using
     * {@link MarshalledValueInputStream}.
     *
     * @param bytes serialized form of an object
     * @return the object, or <code>null</code> if <code>bytes</code>
     *         is <code>null</code>
     */
    public static Object objectFromByteBuffer(byte[] bytes) throws Exception
    {
        if (bytes == null)
        {
            return null;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        MarshalledValueInputStream input = new MarshalledValueInputStream(bais);
        Object result = input.readObject();
        input.close();
        return result;
    }

    /**
     * Creates an object from a byte buffer using
     * {@link MarshalledValueInputStream}.
     *
     * @param bytes serialized form of an object
     * @return the object, or <code>null</code> if <code>bytes</code>
     *         is <code>null</code>
     */
    public static Object objectFromByteBuffer(InputStream bytes) throws Exception
    {
        if (bytes == null)
        {
            return null;
        }

        MarshalledValueInputStream input = new MarshalledValueInputStream(bytes);
        Object result = input.readObject();
        input.close();
        return result;
    }

    /**
     * Serializes an object into a byte buffer using
     * {@link MarshalledValueOutputStream}.
     *
     * @param obj an object that implements Serializable or Externalizable
     * @return serialized form of the object
     */
    public static byte[] objectToByteBuffer(Object obj) throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MarshalledValueOutputStream out = new MarshalledValueOutputStream(baos);
        out.writeObject(obj);
        out.close();
        return baos.toByteArray();
    }

}
