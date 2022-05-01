package org.jboss.cache.loader.rmi;

import org.jboss.cache.TreeCacheMBean;
import org.jboss.system.ServiceMBean;

import javax.management.MalformedObjectNameException;
import java.net.UnknownHostException;

/**
 * @author Manik Surtani
 * @version $Id: RmiCacheServerMBean.java 1121 2006-02-03 16:37:10Z msurtani $
 */
public interface RmiCacheServerMBean extends ServiceMBean {
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

   String getBindName();

   void setBindName(String s);
}
