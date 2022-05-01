/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.w3c.dom.Element;

import java.util.Map;

/**
 * MBean interface for PojoCache. Note that it also inherits from {@link org.jboss.cache.TreeCacheMBean}.
 * @author Ben Wang
 * @since 1.4
 */
public interface PojoCacheMBean extends org.jboss.cache.TreeCacheMBean, PojoCacheIfc {
   /**
    * Inject the config element that is specific to PojoCache.
    * @param config
    * @throws CacheException
    */
   public void setPojoCacheConfig(Element config) throws CacheException;

   public Element getPojoCacheConfig();

}
