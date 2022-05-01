/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jboss.cache.Fqn;


/**
 *  Base cache interceptor
 * @author Ben Wang
 */

public interface BaseInterceptor
      extends org.jboss.aop.advice.Interceptor
{
   /**
    * Get the original fqn that is associated with this interceptor (or advisor).
    *
    */
   Fqn getFqn();

   void setFqn(Fqn fqn);

   AOPInstance getAopInstance();

   void setAopInstance(AOPInstance aopInstance);
}
