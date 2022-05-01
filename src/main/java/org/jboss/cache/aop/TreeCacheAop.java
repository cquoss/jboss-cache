/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jgroups.JChannel;

/**
 * @author Harald Gliebe
 * @author Ben Wang
 * @deprecated Since 1.4, replaced by {@link PojoCache}
 */
public class TreeCacheAop extends PojoCache implements TreeCacheAopMBean
{
   public TreeCacheAop(String cluster_name,
                       String props,
                       long state_fetch_timeout)
           throws Exception {
      super(cluster_name, props, state_fetch_timeout);
   }

   public TreeCacheAop() throws Exception
   {
   }

   public TreeCacheAop(JChannel channel) throws Exception
   {
      super(channel);
   }

}

