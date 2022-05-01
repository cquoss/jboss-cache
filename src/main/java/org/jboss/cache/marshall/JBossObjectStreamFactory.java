package org.jboss.cache.marshall;

import org.jboss.serial.io.JBossObjectInputStreamSharedTree;
import org.jboss.serial.io.JBossObjectOutputStreamSharedTree;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * @author Clebert Suconic
 * @since 1.4.1
 */
class JBossObjectStreamFactory implements ObjectStreamFactory
{
   static class JBossObjectInputStreamOverride extends JBossObjectInputStreamSharedTree
   {

      public JBossObjectInputStreamOverride(InputStream input) throws IOException
      {
         super(input);
      }

      public Object readObjectOverride() throws IOException, ClassNotFoundException
      {
         ClassLoader older = this.getClassLoader();
         try
         {
            this.setClassLoader(Thread.currentThread().getContextClassLoader());
            return super.readObjectOverride();
         }
         finally
         {
            this.setClassLoader(older);
         }
      }

   }


   public ObjectInputStream createObjectInputStream(byte[] bytes) throws IOException
   {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      return new JBossObjectInputStreamOverride(in);
   }

   public ObjectOutputStream createObjectOutputStream(OutputStream out) throws IOException
   {
      return new JBossObjectOutputStreamSharedTree(out);
   }

}
