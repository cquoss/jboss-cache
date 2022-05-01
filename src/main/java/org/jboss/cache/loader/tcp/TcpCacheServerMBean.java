package org.jboss.cache.loader.tcp;

import org.jboss.cache.TreeCacheMBean;
import org.jboss.system.ServiceMBean;

import javax.management.MalformedObjectNameException;
import java.net.UnknownHostException;

/**
 * @author Bela Ban
 * @version $Id: TcpCacheServerMBean.java 1040 2006-01-18 10:44:41Z bela $
 */
public interface TcpCacheServerMBean extends ServiceMBean {
   String getBindAddress();

   void setBindAddress(String bind_addr) throws UnknownHostException;

   int getPort();

   void setPort(int port);

   String getMBeanServerName();

   void setMBeanServerName(String name);

   String getConfig();

   void setConfig(String config);

   TreeCacheMBean getCache();

   void setCache(TreeCacheMBean cache);

   String getCacheName();

   void setCacheName(String cache_name) throws MalformedObjectNameException;

   String getConnections();
}
