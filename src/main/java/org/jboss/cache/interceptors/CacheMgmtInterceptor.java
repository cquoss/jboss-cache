/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.cache.interceptors;

import org.jboss.cache.*;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jgroups.blocks.MethodCall;
import org.jgroups.View;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;

/**
 * Captures cache management statistics
 * @author Jerry Gauthier
 * @version $Id: CacheMgmtInterceptor.java 2043 2006-06-06 10:20:05Z msurtani $
 */
public class CacheMgmtInterceptor extends Interceptor implements CacheMgmtInterceptorMBean, NotificationBroadcaster
{
   
   // Notification Types
   public static final String NOTIF_CACHE_STARTED = "org.jboss.cache.CacheStarted";
   public static final String NOTIF_CACHE_STOPPED = "org.jboss.cache.CacheStopped";
   public static final String NOTIF_NODE_CREATED = "org.jboss.cache.NodeCreated";
   public static final String NOTIF_NODE_EVICTED = "org.jboss.cache.NodeEvicted";
   public static final String NOTIF_NODE_LOADED = "org.jboss.cache.NodeLoaded";
   public static final String NOTIF_NODE_MODIFIED = "org.jboss.cache.NodeModified";
   public static final String NOTIF_NODE_REMOVED = "org.jboss.cache.NodeRemoved";
   public static final String NOTIF_NODE_VISITED = "org.jboss.cache.NodeVisited";
   public static final String NOTIF_VIEW_CHANGE = "org.jboss.cache.ViewChange";
   public static final String NOTIF_NODE_ACTIVATE = "org.jboss.cache.NodeActivate";
   public static final String NOTIF_NODE_EVICT = "org.jboss.cache.NodeEvict";
   public static final String NOTIF_NODE_MODIFY = "org.jboss.cache.NodeModify";
   public static final String NOTIF_NODE_PASSIVATE = "org.jboss.cache.NodePassivate";
   public static final String NOTIF_NODE_REMOVE = "org.jboss.cache.NodeRemove";
   
   // Notification Messages
   private static final String MSG_CACHE_STARTED = "Cache has been started.";
   private static final String MSG_CACHE_STOPPED = "Cache has been stopped.";
   private static final String MSG_NODE_CREATED = "Node has been created.";
   private static final String MSG_NODE_EVICTED = "Node has been evicted.";
   private static final String MSG_NODE_LOADED = "Node has been loaded.";
   private static final String MSG_NODE_MODIFIED = "Node has been modifed.";
   private static final String MSG_NODE_REMOVED = "Node has been removed.";
   private static final String MSG_NODE_VISITED = "Node has been visited.";
   private static final String MSG_VIEW_CHANGE = "Cache cluster view has changed.";
   private static final String MSG_NODE_ACTIVATE = "Node about to be activated.";
   private static final String MSG_NODE_ACTIVATED = "Node has been activated.";
   private static final String MSG_NODE_EVICT = "Node about to be evicted.";
   private static final String MSG_NODE_MODIFY = "Node about to be modified.";
   private static final String MSG_NODE_PASSIVATE = "Node about to be passivated.";
   private static final String MSG_NODE_PASSIVATED = "Node has been passivated.";
   private static final String MSG_NODE_REMOVE = "Node about to be removed.";
   
   // Notification Info 
   private static final String NOTIFICATION_NAME = Notification.class.getName();
   private static final String NOTIFICATION_DESCR = "JBossCache event notifications";
   
   private SynchronizedLong m_seq = new SynchronizedLong(0);
   private int m_listeners = 0;
   private long m_hit_times = 0;
   private long m_miss_times = 0;
   private long m_store_times = 0;
   private long m_hits = 0;
   private long m_misses = 0;
   private long m_stores = 0;
   private long m_evictions = 0;
   private long m_start = System.currentTimeMillis();
   private long m_reset = m_start;
   private CacheMgmtListener m_listener = new CacheMgmtListener();
   private NotificationBroadcasterSupport m_broadcaster = null;
   
   public void setCache(TreeCache cache)
   {
      super.setCache(cache);
      m_broadcaster = new NotificationBroadcasterSupport();
   }

   /**
    * Pass the method on and capture cache statistics
    * @param m
    * @return
    * @throws Throwable
    */
   public Object invoke(MethodCall call) throws Throwable
   {
      //Fqn fqn;
      Map attributes;
      JBCMethodCall m = (JBCMethodCall) call;
      //Object key;
      //Object value;
      Method meth = m.getMethod();
      Object[]args = m.getArgs();
      Object retval = null;


      // if statistics not enabled, execute the method and return
      if (!statsEnabled)
         return super.invoke(m);
      
      long t1, t2;
      switch (m.getMethodId()) 
      {
         case MethodDeclarations.getKeyValueMethodLocal_id:
            //fqn = (Fqn) args[0];
            //key = args[1];
            t1 = System.currentTimeMillis();
            retval=super.invoke(m);
            t2 = System.currentTimeMillis();
            if (retval == null)
            {
               m_miss_times = m_miss_times + (t2 - t1);
               m_misses++;
            }
            else
            {
               m_hit_times = m_hit_times + (t2 - t1);
               m_hits++;
            }
            break;
         case MethodDeclarations.putKeyValMethodLocal_id:
         case MethodDeclarations.putFailFastKeyValueMethodLocal_id:
            //fqn = (Fqn) args[1];
            //key = args[2];
            //value = args[3];
            t1 = System.currentTimeMillis();
            retval=super.invoke(m);
            t2 = System.currentTimeMillis();
            m_store_times = m_store_times + (t2 - t1);
            m_stores++;
            break;
         case MethodDeclarations.putDataMethodLocal_id:
         case MethodDeclarations.putDataEraseMethodLocal_id:
            //fqn = (Fqn) args[1];
            attributes = (Map)args[2];
            t1 = System.currentTimeMillis();
            retval=super.invoke(m);
            t2 = System.currentTimeMillis();
            
            if (attributes != null  && attributes.size() > 0)
            {
               m_store_times = m_store_times + (t2 - t1);
               m_stores = m_stores + attributes.size();            
            }
            break;
         case MethodDeclarations.evictNodeMethodLocal_id:
         case MethodDeclarations.evictVersionedNodeMethodLocal_id:
            //fqn = (Fqn) args[0];
            retval=super.invoke(m);
            m_evictions++;  
            break;
         default :
            retval=super.invoke(m);
            break;
      }
      
      return retval;
   }
   
   public long getHits()
   {
      return m_hits;  
   }
   
   public long getMisses()
   {
      return m_misses;  
   }
   
   public long getStores()
   {
      return m_stores;  
   }
   
   public long getEvictions()
   {
      return m_evictions;  
   }
   
   public double getHitMissRatio()
   {
      double total = m_hits + m_misses;
      if (total == 0)
         return 0;
      return (m_hits/total);  
   }
   
   public double getReadWriteRatio()
   {
      if (m_stores == 0)
         return 0;
      return (((double)(m_hits + m_misses)/(double)m_stores));  
   }
   
   public long getAverageReadTime()
   {
      long total = m_hits + m_misses;
      if (total == 0)
         return 0;
      return (m_hit_times + m_miss_times)/total;  
   }
   
   public long getAverageWriteTime()
   {
      if (m_stores == 0)
         return 0;
      return (m_store_times)/m_stores;  
   }
   
   public int getNumberOfAttributes()
   {
      return cache.getNumberOfAttributes();
   }
   
   public int getNumberOfNodes()
   {
      return cache.getNumberOfNodes();
   }
   
   public long getElapsedTime()
   {
      return (System.currentTimeMillis() - m_start) / 1000;
   }
   
   public long getTimeSinceReset()
   {
      return (System.currentTimeMillis() - m_reset) / 1000;
   }
   
   public Map dumpStatistics()
   {
      Map retval=new HashMap();
      retval.put("Hits", new Long(m_hits));
      retval.put("Misses", new Long(m_misses));
      retval.put("Stores", new Long(m_stores));
      retval.put("Evictions", new Long(m_evictions));
      retval.put("NumberOfAttributes", new Integer(cache.getNumberOfAttributes()));
      retval.put("NumberOfNodes", new Integer(cache.getNumberOfNodes()));
      retval.put("ElapsedTime", new Long(getElapsedTime()));
      retval.put("TimeSinceReset", new Long(getTimeSinceReset()));
      retval.put("AverageReadTime", new Long(getAverageReadTime()));
      retval.put("AverageWriteTime", new Long(getAverageWriteTime()));
      retval.put("HitMissRatio", new Double(getHitMissRatio()));
      retval.put("ReadWriteRatio", new Double(getReadWriteRatio()));
      return retval;
   }
   
   public void resetStatistics()
   {
      m_hits = 0;
      m_misses = 0;
      m_stores = 0;
      m_evictions = 0;
      m_hit_times = 0;
      m_miss_times = 0;
      m_store_times = 0;
      m_reset = System.currentTimeMillis();
   }
   
   private synchronized void emitNotifications(boolean emit)
   {
      // This method adds and removes the TreeCache listener.
      // The m_listeners counter is used to determine whether
      // we have any clients who are registered for notifications
      // from this mbean.  When the count is zero, we don't need to 
      // listen to cache events.  Note that a client who terminates
      // without unregistering for notifications will leave the count
      // greater than zero so we'll still listen in that case.
      if (emit)
      {
         m_listeners++;
         cache.addTreeCacheListener(m_listener);
      }
      else
      {
         m_listeners--;
         if (m_listeners <= 0)
         {
            cache.removeTreeCacheListener(m_listener);
         }
      }
   }
   
   // NotificationBroadcaster interface
   
   public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
      throws ListenerNotFoundException
   {
      m_broadcaster.removeNotificationListener(listener, filter, handback);
   }
   
   public MBeanNotificationInfo[] getNotificationInfo()
   {
      String[] types = new String[] {NOTIF_CACHE_STARTED, NOTIF_CACHE_STOPPED, NOTIF_NODE_CREATED,
                                    NOTIF_NODE_EVICTED, NOTIF_NODE_LOADED, NOTIF_NODE_MODIFIED, 
                                    NOTIF_NODE_REMOVED, NOTIF_NODE_VISITED, NOTIF_VIEW_CHANGE, 
                                    NOTIF_NODE_ACTIVATE, NOTIF_NODE_EVICT, NOTIF_NODE_MODIFY, 
                                    NOTIF_NODE_PASSIVATE, NOTIF_NODE_REMOVE}; 
      MBeanNotificationInfo info = new MBeanNotificationInfo(types, NOTIFICATION_NAME, NOTIFICATION_DESCR);
      return new MBeanNotificationInfo[] {info}; 
   }   
   
   public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
      throws IllegalArgumentException
   {
      m_broadcaster.addNotificationListener(listener, filter, handback);
      emitNotifications(true);
   }

   public void removeNotificationListener(NotificationListener listener)
      throws ListenerNotFoundException
   {
      m_broadcaster.removeNotificationListener(listener);
      emitNotifications(false);
   }
   
  // Handler for TreeCache events
   private class CacheMgmtListener extends AbstractTreeCacheListener
   {
      CacheMgmtListener()
      {
      }

      private long seq() {
        return m_seq.increment();
      }
      
      public void cacheStarted(TreeCache cache)
      {
         Notification n = new Notification(NOTIF_CACHE_STARTED, this, seq(), MSG_CACHE_STARTED);
         n.setUserData(cache.getServiceName());
         m_broadcaster.sendNotification(n);
      }
      
      public void cacheStopped(TreeCache cache)
      {
         Notification n = new Notification(NOTIF_CACHE_STOPPED, this, seq(), MSG_CACHE_STOPPED);
         n.setUserData(cache.getServiceName());
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeCreated(Fqn fqn)
      {
         Notification n = new Notification(NOTIF_NODE_CREATED, this, seq(), MSG_NODE_CREATED);
         n.setUserData(fqn.toString());
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeEvicted(Fqn fqn)
      {
         Notification n = new Notification(NOTIF_NODE_EVICTED, this, seq(), MSG_NODE_EVICTED);
         n.setUserData(fqn.toString());
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeLoaded(Fqn fqn)
      {
         Notification n = new Notification(NOTIF_NODE_LOADED, this, seq(), MSG_NODE_LOADED);
         n.setUserData(fqn.toString());
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeModified(Fqn fqn)
      {
         Notification n = new Notification(NOTIF_NODE_MODIFIED, this, seq(), MSG_NODE_MODIFIED);
         n.setUserData(fqn.toString());
         m_broadcaster.sendNotification(n);
      }
    
      public void nodeRemoved(Fqn fqn)
      {
         Notification n = new Notification(NOTIF_NODE_REMOVED, this, seq(), MSG_NODE_REMOVED);
         n.setUserData(fqn.toString());
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeVisited(Fqn fqn)
      {
         Notification n = new Notification(NOTIF_NODE_VISITED, this, seq(), MSG_NODE_VISITED);
         n.setUserData(fqn.toString());
         m_broadcaster.sendNotification(n);
      }
      
      public void viewChange(View view)
      {
         Notification n = new Notification(NOTIF_VIEW_CHANGE, this, seq(), MSG_VIEW_CHANGE);
         n.setUserData(view.toString());
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeActivate(Fqn fqn, boolean pre)
      {
         Notification n;
         if (pre)
            n = new Notification(NOTIF_NODE_ACTIVATE, this, seq(), MSG_NODE_ACTIVATE);
         else
            n = new Notification(NOTIF_NODE_ACTIVATE, this, seq(), MSG_NODE_ACTIVATED);
         n.setUserData(new Object[]{fqn.toString(), Boolean.valueOf(pre)});
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeEvict(Fqn fqn, boolean pre)
      {
         Notification n;
         if (pre)
            n = new Notification(NOTIF_NODE_EVICT, this, seq(), MSG_NODE_EVICT);
         else
            n = new Notification(NOTIF_NODE_EVICT, this, seq(), MSG_NODE_EVICTED);
         n.setUserData(new Object[]{fqn.toString(), Boolean.valueOf(pre)});
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeModify(Fqn fqn, boolean pre, boolean isLocal)
      {
         Notification n;
         if (pre)
            n = new Notification(NOTIF_NODE_MODIFY, this, seq(), MSG_NODE_MODIFY);
         else
            n = new Notification(NOTIF_NODE_MODIFY, this, seq(), MSG_NODE_MODIFIED);
         n.setUserData(new Object[]{fqn.toString(), Boolean.valueOf(pre), Boolean.valueOf(isLocal)});
         m_broadcaster.sendNotification(n);
      }
    
      public void nodePassivate(Fqn fqn, boolean pre)
      {
         Notification n;
         if (pre)
            n = new Notification(NOTIF_NODE_PASSIVATE, this, seq(), MSG_NODE_PASSIVATE);
         else
            n = new Notification(NOTIF_NODE_PASSIVATE, this, seq(), MSG_NODE_PASSIVATED);
         n.setUserData(new Object[]{fqn.toString(), Boolean.valueOf(pre)});
         m_broadcaster.sendNotification(n);
      }
      
      public void nodeRemove(Fqn fqn, boolean pre, boolean isLocal)
      {
         Notification n;
         if (pre)
            n = new Notification(NOTIF_NODE_REMOVE, this, seq(), MSG_NODE_REMOVE);
         else
            n = new Notification(NOTIF_NODE_REMOVE, this, seq(), MSG_NODE_REMOVED);
         n.setUserData(new Object[]{fqn.toString(), Boolean.valueOf(pre), Boolean.valueOf(isLocal)});
         m_broadcaster.sendNotification(n);
      }
  
   }
}

