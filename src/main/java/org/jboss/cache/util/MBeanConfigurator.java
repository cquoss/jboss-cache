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
package org.jboss.cache.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.TreeCache;
import org.jboss.cache.interceptors.BaseInterceptor;
import org.jboss.cache.interceptors.Interceptor;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.List;

/**
 * JBoss cache management configurator
 * @author Jerry Gauthier
 * @version $Id: MBeanConfigurator.java 3761 2007-04-19 22:04:46Z jerrygauth $
 */
public class MBeanConfigurator
{
   private static final String MBEAN_CLASS_SUFFIX = "MBean";
   private static final String MBEAN_KEY = ",treecache-interceptor=";
   private static final String PREFIX = "jboss.cache:service=";
   private static final Log LOG = LogFactory.getLog(MBeanConfigurator.class);
   
   /*
    * Register the associated mbeans for cache interceptors
    * 
    * @param server the mbean server with which the mbeans should be registered
    * @param cache the cache having the set of interceptors to be registered
    * @param registerCache whether the cache itself should be registered
    */
   public static void registerInterceptors(MBeanServer server, TreeCache cache, boolean registerCache)
      throws Exception
   {
      if (server == null || cache == null)
         return;
      
      List interceptors = cache.getInterceptors();
      Interceptor interceptor = null;
      
      // get the cache's registration name
      String tmpName = cache.getServiceName() != null? cache.getServiceName().toString() : null;
      if(tmpName == null)
      {         
         tmpName = PREFIX + cache.getClusterName();
         if(cache.getClusterName() == null)
            tmpName = PREFIX + cache.getClass().getName() + System.currentTimeMillis();
      }
      // register the cache
      if (registerCache)
      {
         ObjectName tmpObj = new ObjectName(tmpName);
         if (!server.isRegistered(tmpObj))
         {
            // under some circumstances, this block may be executed on WebSphere when the name is already registered
            try
            {
               server.registerMBean(cache, tmpObj);
            }
            catch (Exception e)
            {
               LOG.warn("mbean registration failed for " + tmpObj.getCanonicalName() + "; " + e.toString());
            }
         }
         else
         {
            LOG.warn("mbean " + tmpObj.getCanonicalName() + " already registered");
         }
      }

      for(int i = 0; i < interceptors.size(); i++)
      {
         interceptor =(Interceptor)interceptors.get(i);
         boolean mbeanExists = true;
         try
         {
            // the mbean for interceptor AbcInterceptor will be named AbcInterceptorMBean
            Class.forName(interceptor.getClass().getName()+ MBEAN_CLASS_SUFFIX);
         }
         catch(Throwable e)
         {
            // if the class can't be instantiated, no mbean is available
            mbeanExists = false;
         }

         // for JDK 1.4, must parse name and remove package prefix
         // for JDK 1.5, can use getSimpleName() to establish class name without package prefix
         String className = interceptor.getClass().getName();
         String serviceName = tmpName + MBEAN_KEY + className.substring(className.lastIndexOf('.')+1);
         
         ObjectName objName = new ObjectName(serviceName);
         if (!server.isRegistered(objName))
         {
            // under some circumstances, this block may be executed on WebSphere when the name is already registered
            try
            {
               if (mbeanExists)
                  // register associated interceptor mbean
                  server.registerMBean(interceptor, objName);
               else
                  // register dummy interceptor mbean
                  server.registerMBean(new BaseInterceptor(), objName);
            }
            catch (Exception e)
            {
               LOG.warn("mbean registration failed for " + objName.getCanonicalName() + "; " + e.toString());
            }            
         }
         else
         {
            LOG.warn("mbean " + objName.getCanonicalName() + " already registered");
         }
      }
   }
   
   /*
    * Unregister the associated mbeans for cache interceptors
    * 
    * @param server the mbean server for which the mbeans should be unregistered
    * @param cache the cache having the set of interceptors to be unregistered
    * @param unregisterCache whether the cache itself should be unregistered
    */
   public static void unregisterInterceptors(MBeanServer server, TreeCache cache, boolean unregisterCache)
      throws Exception
   {
      if (server == null || cache == null)
         return;
      
      List interceptors = cache.getInterceptors();
      Interceptor interceptor = null;
      
      // get the cache's registration name
      String tmpName = cache.getServiceName() != null? cache.getServiceName().toString() : null;
      if(tmpName == null)
      {         
         tmpName = PREFIX + cache.getClusterName();
         if(cache.getClusterName() == null)
            tmpName = PREFIX + cache.getClass().getName() + System.currentTimeMillis();
      }

      for(int i = 0; i < interceptors.size(); i++)
      {
         interceptor =(Interceptor)interceptors.get(i);

         // for JDK 1.4, must parse name and remove package prefix
         // for JDK 1.5, can use getSimpleName() to establish class name without package prefix
         String className = interceptor.getClass().getName();
         String serviceName = tmpName + MBEAN_KEY + className.substring(className.lastIndexOf('.')+1);
         
         ObjectName objName = new ObjectName(serviceName);
         if (server.isRegistered(objName))
            try
            {
               server.unregisterMBean(objName);
            }
            catch (Exception e)
            {
               // this shouldn't occur; if it does somehow, we don't want to stop the deregistration process
               LOG.warn("mbean deregistration failed for " + objName.getCanonicalName() + "; " + e.toString());
            }
      }
      
      // unregister the cache itself
      if (unregisterCache)
      {
         ObjectName tmpObj = new ObjectName(tmpName);
         if (server.isRegistered(tmpObj))
            try
            {
               server.unregisterMBean(tmpObj);
            }
            catch (Exception e)
            {
               // this shouldn't occur; if it does somehow, we don't want to stop the deregistration process
               LOG.warn("mbean deregistration failed for " + tmpObj.getCanonicalName() + "; " + e.toString());
            }
      }
   }
}
