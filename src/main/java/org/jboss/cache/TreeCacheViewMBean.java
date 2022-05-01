/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;

/**
 * MBean interface.
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> March 27 2003
 */
public interface TreeCacheViewMBean extends org.jboss.system.ServiceMBean {

  void create() throws java.lang.Exception;

  void start() throws java.lang.Exception;

  void stop() ;

  void destroy() ;

  java.lang.String getCacheService() ;

  void setCacheService(java.lang.String cache_service) throws java.lang.Exception;

}
