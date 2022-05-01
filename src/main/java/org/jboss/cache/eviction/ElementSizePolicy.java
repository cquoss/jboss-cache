/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

/**
 * @author Daniel Huang
 * @version $Revison: $
 */
public class ElementSizePolicy extends BaseEvictionPolicy implements EvictionPolicy
{
   private ElementSizeAlgorithm algorithm;

   public ElementSizePolicy()
   {
      super();
      algorithm = new ElementSizeAlgorithm();
   }

   public EvictionAlgorithm getEvictionAlgorithm()
   {
      return this.algorithm;
   }

   public Class getEvictionConfigurationClass()
   {
      return ElementSizeConfiguration.class;
   }
}
