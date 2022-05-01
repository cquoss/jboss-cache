/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;

import java.util.Map;

/**
 * MBean interface.
 * @author Harald Gliebe
 * @author Ben Wang
 * @deprecated Since 1.4, replaced by {@link PojoCacheMBean}
 */
public interface TreeCacheAopMBean extends PojoCacheMBean, TreeCacheAopIfc
{
}
