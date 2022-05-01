/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

/**
 * Eviction policy based on the FIFO algorithm where users can specify the max
 * number of nodes.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @author Morten Kvistgaard
 * @version $Revision: 900 $
 */
public class FIFOPolicy extends BaseEvictionPolicy implements EvictionPolicy
{
   private FIFOAlgorithm algorithm;

   public FIFOPolicy()
   {
      super();
      algorithm = new FIFOAlgorithm();
   }

   public EvictionAlgorithm getEvictionAlgorithm()
   {
      return algorithm;
   }

   public Class getEvictionConfigurationClass()
   {
      return FIFOConfiguration.class;
   }
}
