package org.jboss.cache.loader.tcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.PropertyConfigurator;
import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeCacheMBean;
import org.jboss.cache.loader.DelegatingCacheLoader;
import org.jboss.mx.util.MBeanProxyExt;
import org.jboss.system.ServiceMBeanSupport;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TCP-IP based CacheServer, configure TcpDelegatingCacheLoader with host and port of this server
 *
 * @author Bela Ban
 * @version $Id: TcpCacheServer.java 2736 2006-10-25 12:42:54Z msurtani $
 */
public class TcpCacheServer extends ServiceMBeanSupport implements TcpCacheServerMBean
{
   ServerSocket srv_sock;
   InetAddress bind_addr = null;
   int port = 7500;
   TreeCacheMBean cache;
   ObjectName cache_name;
   String config;
   boolean running = true;
   final List conns = new LinkedList();
   String agendId;
   Thread serverThread;
   /**
    * whether or not to start the server thread as a daemon.  Should be false if started from the command line, true if started as an MBean.
    */
   boolean daemon = true;
   static Log mylog = LogFactory.getLog(TcpCacheServer.class);


   public TcpCacheServer()
   {
   }

   public String getBindAddress()
   {
      return bind_addr != null ? bind_addr.toString() : "n/a";
   }

   public void setBindAddress(String bind_addr) throws UnknownHostException
   {
      if (bind_addr != null)
         this.bind_addr = InetAddress.getByName(bind_addr);
   }

   public int getPort()
   {
      return port;
   }

   public void setPort(int port)
   {
      this.port = port;
   }

   public String getMBeanServerName()
   {
      return agendId;
   }

   public void setMBeanServerName(String name)
   {
      agendId = name;
   }

   public String getConfig()
   {
      return config;
   }

   public void setConfig(String config)
   {
      this.config = config;
   }

   public TreeCacheMBean getCache()
   {
      return cache;
   }

   public void setCache(TreeCacheMBean cache)
   {
      this.cache = cache;
   }

   public String getCacheName()
   {
      return cache_name != null ? cache_name.toString() : "n/a";
   }

   public void setCacheName(String cache_name) throws MalformedObjectNameException
   {
      this.cache_name = new ObjectName(cache_name);
   }

   public void createService() throws Exception
   {
      super.createService();
   }

   public void startService() throws Exception
   {

      if (cache == null)
      {
         // 1. check whether we have an object name, pointing to the cache MBean
         if (cache_name != null && server != null)
         {
            cache = (TreeCacheMBean) MBeanProxyExt.create(TreeCacheMBean.class, cache_name, server);
         }
      }

      if (cache == null)
      { // still not set
         if (config != null)
         {
            cache = new TreeCache();
            PropertyConfigurator cfg = new PropertyConfigurator();
            cfg.configure(cache, config);
            cache.createService();
            cache.startService();
         }
      }

      if (cache == null)
         throw new CacheException("cache reference is not set");


      srv_sock = new ServerSocket(port, 10, bind_addr);
      System.out.println("TcpCacheServer listening on : " + srv_sock.getInetAddress() + ":" + srv_sock.getLocalPort());
      mylog.info("TcpCacheServer listening on : " + srv_sock.getInetAddress() + ":" + srv_sock.getLocalPort());

      running = true;

      serverThread = new Thread("TcpCacheServer")
      {
         public void run()
         {
            try
            {
               while (running)
               {
                  Socket client_sock = srv_sock.accept();
                  Connection conn = new Connection(client_sock, cache);
                  conns.add(conn);
                  conn.start();
               }
            }
            catch (SocketException se)
            {
               if (!running)
               {
                  // this is because of the stop() lifecycle method being called.
                  // ignore.
                  mylog.info("Shutting down TcpCacheServer");
               }
               else
               {
                  mylog.error("Caught exception! Shutting down server thread.", se);
               }
            }
            catch (IOException e)
            {
               mylog.error("Caught exception! Shutting down server thread.", e);
            }
         }
      };
      serverThread.setDaemon(daemon);
      serverThread.start();

   }

   public void stopService()
   {
      running = false;
      for (Iterator it = conns.iterator(); it.hasNext();)
      {
         Connection conn = (Connection) it.next();
         conn.close();
      }
      conns.clear();

      if (srv_sock != null)
      {
         try
         {
            srv_sock.close();
            srv_sock = null;
         }
         catch (IOException e)
         {
         }
      }
   }


   public String getConnections()
   {
      StringBuffer sb = new StringBuffer();
      sb.append(conns.size() + " connections:\n");
      for (Iterator it = conns.iterator(); it.hasNext();)
      {
         Connection c = (Connection) it.next();
         sb.append(c).append("\n");
      }
      return sb.toString();
   }


   public void destroy()
   {
      super.destroy();
   }


   private class Connection implements Runnable
   {
      Socket sock = null;
      ObjectInputStream input = null;
      ObjectOutputStream output = null;
      TreeCacheMBean c;
      Thread t = null;

      public Connection(Socket sock, TreeCacheMBean cache) throws IOException
      {
         this.sock = sock;

         output = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream()));
         output.flush();

         input = new ObjectInputStream(new BufferedInputStream(sock.getInputStream()));

         c = cache;
      }


      public void start()
      {
         t = new Thread(this, "TcpCacheServer.Connection");
         t.setDaemon(true);
         t.start();
      }

      public void close()
      {
         t = null;
         try {if (output != null) output.close();} catch (Throwable th) {}
         try {if (input != null) input.close();} catch (Throwable th) {}
         try {if (sock != null) sock.close();} catch (Throwable th) {}

         // remove self from connections list
         conns.remove(this);
      }

      public void run()
      {
         int op;
         Fqn fqn;
         Object key, val, retval;
         Map map;
         DataNode n;
         boolean flag;
         byte[] state;

         while (t != null && Thread.currentThread().equals(t))
         {
            try
            {
               op = input.readInt();
            }
            catch (IOException e)
            {
               mylog.debug("Client closed socket");
               close();
               break;
            }

            try
            {
               output.reset();
               switch (op)
               {
                  case DelegatingCacheLoader.delegateGetChildrenNames:
                     fqn = (Fqn) input.readObject();
                     Set children = c.getChildrenNames(fqn);
                     output.writeObject(children);  // this may be null - that's okay
                     break;
                  case DelegatingCacheLoader.delegateGetKey:
                     fqn = (Fqn) input.readObject();
                     key = input.readObject();
                     retval = c.get(fqn, key);
                     output.writeObject(retval);
                     break;
                  case DelegatingCacheLoader.delegateGet:
                     fqn = (Fqn) input.readObject();
                     n = c.get(fqn);
                     if (n == null)
                     { // node doesn't exist - return null
                        output.writeObject(n);
                        break;
                     }
                     map = n.getData();
                     if (map == null) map = new HashMap();
                     output.writeObject(map);
                     break;
                  case DelegatingCacheLoader.delegateExists:
                     fqn = (Fqn) input.readObject();
                     flag = c.exists(fqn);
                     output.writeObject(Boolean.valueOf(flag));
                     break;
                  case DelegatingCacheLoader.delegatePutKeyVal:
                     fqn = (Fqn) input.readObject();
                     key = input.readObject();
                     val = input.readObject();
                     retval = c.put(fqn, key, val);
                     output.writeObject(retval);
                     break;
                  case DelegatingCacheLoader.delegatePut:
                     fqn = (Fqn) input.readObject();
                     map = (Map) input.readObject();
                     c.put(fqn, map);
                     output.writeObject(Boolean.TRUE);
                     break;

                  case DelegatingCacheLoader.putList:
                     int length = input.readInt();
                     retval = Boolean.TRUE;
                     if (length > 0)
                     {
                        Modification mod;
                        List mods = new ArrayList(length);
                        for (int i = 0; i < length; i++)
                        {
                           mod = new Modification();
                           mod.readExternal(input);
                           mods.add(mod);
                        }
                        try
                        {
                           handleModifications(mods);
                        }
                        catch (Exception ex)
                        {
                           retval = ex;
                        }
                     }
                     output.writeObject(retval);
                     break;
                  case DelegatingCacheLoader.delegateRemoveKey:
                     fqn = (Fqn) input.readObject();
                     key = input.readObject();
                     retval = c.remove(fqn, key);
                     output.writeObject(retval);
                     break;
                  case DelegatingCacheLoader.delegateRemove:
                     fqn = (Fqn) input.readObject();
                     c.remove(fqn);
                     output.writeObject(Boolean.TRUE);
                     break;
                  case DelegatingCacheLoader.delegateRemoveData:
                     fqn = (Fqn) input.readObject();
                     c.removeData(fqn);
                     output.writeObject(Boolean.TRUE);
                     break;
                  case DelegatingCacheLoader.delegateLoadEntireState:
                     state = c.getCacheLoader() != null ? c.getCacheLoader().loadEntireState() : null;
                     output.writeObject(state);
                     break;
                  case DelegatingCacheLoader.delegateStoreEntireState:
                     state = (byte[]) input.readObject();
                     if (c.getCacheLoader() != null)
                        c.getCacheLoader().storeEntireState(state);
                     output.writeObject(Boolean.TRUE);
                     break;
                  default:
                     mylog.error("Operation " + op + " unknown");
                     break;
               }
               output.flush();
            }
            catch (Exception e)
            {
               try
               {
                  output.writeObject(e);
                  output.flush();
               }
               catch (IOException e1)
               {
                  e1.printStackTrace();
               }
            }
         }
      }


      public String toString()
      {
         StringBuffer sb = new StringBuffer();
         if (sock != null)
         {
            sb.append(sock.getRemoteSocketAddress());
         }
         return sb.toString();
      }

      protected void handleModifications(List modifications) throws CacheException
      {
         for (Iterator it = modifications.iterator(); it.hasNext();)
         {
            Modification m = (Modification) it.next();
            switch (m.getType())
            {
               case Modification.PUT_DATA:
                  c.put(m.getFqn(), m.getData());
                  break;
               case Modification.PUT_DATA_ERASE:
                  c.put(m.getFqn(), m.getData());
                  break;
               case Modification.PUT_KEY_VALUE:
                  c.put(m.getFqn(), m.getKey(), m.getValue());
                  break;
               case Modification.REMOVE_DATA:
                  c.removeData(m.getFqn());
                  break;
               case Modification.REMOVE_KEY_VALUE:
                  c.remove(m.getFqn(), m.getKey());
                  break;
               case Modification.REMOVE_NODE:
                  c.remove(m.getFqn());
                  break;
               default:
                  mylog.error("modification type " + m.getType() + " not known");
                  break;
            }
         }
      }


   }


   public static void main(String[] args) throws Exception
   {

      String bind_addr = null;
      int port = 7500;
      TcpCacheServer server;
      String config = null;

      for (int i = 0; i < args.length; i++)
      {
         if (args[i].equals("-bind_addr"))
         {
            bind_addr = args[++i];
            continue;
         }
         if (args[i].equals("-port"))
         {
            port = Integer.parseInt(args[++i]);
            continue;
         }
         if (args[i].equals("-config"))
         {
            config = args[++i];
            continue;
         }
         help();
         return;
      }
      server = new TcpCacheServer();
      server.daemon = false;
      server.setBindAddress(bind_addr);
      server.setPort(port);
      server.setConfig(config);
      server.createService();
      server.startService();

   }


   private static void help()
   {
      System.out.println("TcpCacheServer [-bind_addr <address>] [-port <port>] [-config <config file>] [-help]");
   }
}
