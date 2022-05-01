/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader.tcp;

import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.TreeCache;
import org.jboss.cache.loader.DelegatingCacheLoader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * DelegatingCacheLoader implementation which delegates to a remote (not in the same VM)
 * TreeCache using TCP/IP for communication. Example configuration for connecting to a TcpCacheServer
 * running at myHost:12345:<pre>
 * <attribute name="CacheLoaderClass">org.jboss.cache.loader.tcp.TcpDelegatingCacheLoader</attribute>
 * <attribute name="CacheLoaderConfig">
 * host=localhost
 * port=2099
 * </attribute>
 * </pre>
 *
 * @author Bela Ban
 * @version $Id: TcpDelegatingCacheLoader.java 2735 2006-10-25 12:16:58Z msurtani $
 */
public class TcpDelegatingCacheLoader extends DelegatingCacheLoader
{
   private Socket sock;
   private String host;
   private int port;
   ObjectInputStream in;
   ObjectOutputStream out;


   /**
    * Default constructor.
    */
   public TcpDelegatingCacheLoader()
   {
      // Empty.
   }

   /**
    * Allows programmatic configuration.
    *
    * @param host The host on which to look up the remote object.
    * @param port The port on which to look up the remote object.
    */
   public TcpDelegatingCacheLoader(String host, int port)
   {
      this.host = host;
      this.port = port;
   }

   /**
    * Allows configuration via XML config file.
    *
    * @see org.jboss.cache.loader.DelegatingCacheLoader#setConfig(java.util.Properties)
    */
   public void setConfig(Properties props)
   {
      this.host = props.getProperty("host");
      if (this.host == null || this.host.length() == 0)
      {
         this.host = "localhost";
      }
      this.port = Integer.parseInt(props.getProperty("port"));
   }

   public void start() throws Exception
   {
      init();
   }

   public void stop()
   {
      synchronized (out)
      {
         try {if (in != null) in.close();} catch (IOException e) {}
         try {if (out != null) out.close();} catch (IOException e) {}
         try {if (sock != null) sock.close();} catch (IOException e) {}
      }
   }


   private void init() throws IOException
   {
      if (host == null)
         host = "localhost";
      sock = new Socket(host, port);
      out = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream()));
      out.flush();
//      out=new ObjectOutputStream(sock.getOutputStream());
//      in=new ObjectInputStream(sock.getInputStream());
      in = new ObjectInputStream(new BufferedInputStream(sock.getInputStream()));
   }

   /**
    * Allows configuration via XML config file.
    *
    * @see org.jboss.cache.loader.DelegatingCacheLoader#setCache(org.jboss.cache.TreeCache)
    */
   public void setCache(TreeCache cache)
   {
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGetChildrenNames(org.jboss.cache.Fqn)
    */
   protected Set delegateGetChildrenNames(Fqn fqn) throws Exception
   {
      synchronized (out)
      {
         out.reset();
         out.writeInt(DelegatingCacheLoader.delegateGetChildrenNames);
         out.writeObject(fqn);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
         return (Set) retval;
      }
   }

   // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGet(org.jboss.cache.Fqn,Object)
    */
//   protected Object delegateGet(Fqn name, Object key) throws Exception {
//      out.writeInt(DelegatingCacheLoader.delegateGetKey);
//      out.writeObject(name);
//      out.writeObject(key);
//      return in.readObject();
//   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateGet(org.jboss.cache.Fqn)
    */
   protected Map delegateGet(Fqn name) throws Exception
   {
      synchronized (out)
      {
         out.reset();
         out.writeInt(DelegatingCacheLoader.delegateGet);
         out.writeObject(name);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
         return (Map) retval;
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateExists(org.jboss.cache.Fqn)
    */
   protected boolean delegateExists(Fqn name) throws Exception
   {
      synchronized (out)
      {
         out.reset();
         out.writeInt(DelegatingCacheLoader.delegateExists);
         out.writeObject(name);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
         return ((Boolean) retval).booleanValue();
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegatePut(org.jboss.cache.Fqn,Object,Object)
    */
   protected Object delegatePut(Fqn name, Object key, Object value) throws Exception
   {
      synchronized (out)
      {

         out.reset();
         out.writeInt(DelegatingCacheLoader.delegatePutKeyVal);
         out.writeObject(name);
         out.writeObject(key);
         out.writeObject(value);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
         return retval;
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegatePut(org.jboss.cache.Fqn,java.util.Map)
    */
   protected void delegatePut(Fqn name, Map attributes) throws Exception
   {
      synchronized (out)
      {

         out.reset();
         out.writeInt(DelegatingCacheLoader.delegatePut);
         out.writeObject(name);
         out.writeObject(attributes);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
      }
   }

   protected void delegatePut(List modifications) throws Exception
   {
      synchronized (out)
      {

         out.reset();
         out.writeInt(DelegatingCacheLoader.putList);
         int length = modifications != null ? modifications.size() : 0;
         out.writeInt(length);
         if (length > 0)
         {
            for (Iterator it = modifications.iterator(); it.hasNext();)
            {
               Modification m = (Modification) it.next();
               m.writeExternal(out);
            }
         }
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemove(org.jboss.cache.Fqn,Object)
    */
   protected Object delegateRemove(Fqn name, Object key) throws Exception
   {
      synchronized (out)
      {

         out.reset();
         out.writeInt(DelegatingCacheLoader.delegateRemoveKey);
         out.writeObject(name);
         out.writeObject(key);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
         return retval;
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemove(org.jboss.cache.Fqn)
    */
   protected void delegateRemove(Fqn name) throws Exception
   {
      synchronized (out)
      {

         out.reset();
         out.writeInt(DelegatingCacheLoader.delegateRemove);
         out.writeObject(name);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateRemoveData(org.jboss.cache.Fqn)
    */
   protected void delegateRemoveData(Fqn name) throws Exception
   {
      synchronized (out)
      {

         out.reset();
         out.writeInt(DelegatingCacheLoader.delegateRemoveData);
         out.writeObject(name);
         out.flush();
         Object retval = in.readObject();
         if (retval instanceof Exception)
            throw(Exception) retval;
      }
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateLoadEntireState()
    */
   public byte[] delegateLoadEntireState() throws Exception
   {
      throw new UnsupportedOperationException("operation is not currently supported - need to define semantics first");
//      out.writeInt(DelegatingCacheLoader.delegateLoadEntireState);
//      out.flush();
//      Object retval=in.readObject();
//      if(retval instanceof Exception)
//         throw (Exception)retval;
//      return (byte[])retval;
   }

   /**
    * @see org.jboss.cache.loader.DelegatingCacheLoader#delegateStoreEntireState(byte[])
    */
   public void delegateStoreEntireState(byte[] state) throws Exception
   {
      throw new UnsupportedOperationException("operation is not currently supported - need to define semantics first");
//      out.writeInt(DelegatingCacheLoader.delegateStoreEntireState);
//      out.writeObject(state);
//      out.flush();
//      Object retval=in.readObject();
//      if(retval instanceof Exception)
//         throw (Exception)retval;
   }


}
