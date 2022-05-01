package org.jboss.cache.marshall;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * @author Clebert Suconic
 * @since 1.4.1
 */
public interface ObjectStreamFactory
{
   public ObjectOutputStream createObjectOutputStream(OutputStream out) throws IOException;

   public ObjectInputStream createObjectInputStream(byte[] bytes) throws IOException;
}
