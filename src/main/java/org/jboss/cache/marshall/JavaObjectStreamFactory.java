package org.jboss.cache.marshall;

import org.jboss.invocation.MarshalledValueInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * @author Clebert Suconic
 * @since 1.4.1
 */
class JavaObjectStreamFactory implements ObjectStreamFactory
{

   public ObjectInputStream createObjectInputStream(byte[] bytes) throws IOException
   {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      return new MarshalledValueInputStream(in);
   }

   public ObjectOutputStream createObjectOutputStream(OutputStream out) throws IOException
   {
      return new ObjectOutputStream(out);
   }

}
