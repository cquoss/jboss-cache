/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.aop.eviction;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.jboss.cache.aop.eviction.AopEvictionPolicy;
//import org.jboss.cache.aop.eviction.AopLRUAlgorithm;
import org.jboss.cache.eviction.LRUPolicy;
import org.jboss.cache.eviction.EvictionAlgorithm;
import org.jboss.cache.Fqn;
import org.jboss.cache.aop.InternalDelegate;


/**
 * Provider to provide eviction policy. This one is based on LRU algorithm that a user
 * can specify either maximum number of nodes or the idle time of a node to be evicted.
 *
 * @author Ben Wang 02-2004
 */
public class AopLRUPolicy extends LRUPolicy implements AopEvictionPolicy
{
//   private static final Log log_ = LogFactory.getLog(AopLRUPolicy.class);

   public AopLRUPolicy()
   {
      super();
      algorithm = (EvictionAlgorithm)new AopLRUAlgorithm();
   }

   // we are using the same eviction algorithm now.
   public EvictionAlgorithm getEvictionAlgorithm()
   {
      return algorithm;
   }

   /**
    * Override to provide PojoCache specific behavior.
    * @param fqn
    */
   public boolean canIgnoreEvent(Fqn fqn) {
      return false;
   }
}
