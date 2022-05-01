/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.marshall;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Factory class for creating object output and inut streams, switching between JDK defaults and JBoss Serialization classes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ObjectSerializationFactory
{

   static boolean useJBossSerialization = false;
   private static Log log = LogFactory.getLog(ObjectSerializationFactory.class);
   static ObjectStreamFactory factory = new JavaObjectStreamFactory();

   static
   {
      String useJBossSerializationStr = System.getProperty("serialization.jboss", "true");
      useJBossSerialization = Boolean.valueOf(useJBossSerializationStr).booleanValue();

      try
      {
         if (useJBossSerialization)
         {
            factory = (ObjectStreamFactory) Class.forName("org.jboss.cache.marshall.JBossObjectStreamFactory").newInstance();
         }
      }
      catch (Exception e)
      {
         log.error("Unable to load JBossObjectStreamFactory.  Perhaps jboss-serialization jar not loaded?", e);
         log.error("Falling back to java serialization.");

      }
   }

   public static ObjectOutputStream createObjectOutputStream(OutputStream out) throws IOException
   {
      return factory.createObjectOutputStream(out);
   }

   public static ObjectInputStream createObjectInputStream(byte[] bytes) throws IOException
   {
      return factory.createObjectInputStream(bytes);
   }

   public static boolean useJBossSerialization()
   {
      return useJBossSerialization;
   }
}
