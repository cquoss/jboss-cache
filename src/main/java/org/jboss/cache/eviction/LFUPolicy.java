/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

/**
 * Least Frequently Used Eviction Policy.
 *
 * @author Daniel Huang - dhuang@jboss.org - 10/2005
 * @version $Revision: 900 $
 */
public class LFUPolicy extends BaseEvictionPolicy implements EvictionPolicy
{
   private LFUAlgorithm algorithm;

   public LFUPolicy()
   {
      super();
      algorithm = new LFUAlgorithm();
   }

   public EvictionAlgorithm getEvictionAlgorithm()
   {
      return algorithm;
   }

   public Class getEvictionConfigurationClass()
   {
      return LFUConfiguration.class;
   }
}
