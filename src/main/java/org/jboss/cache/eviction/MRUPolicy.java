/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

/**
 * Most Recently Used Policy.
 * <p/>
 * This algorithm will evict the most recently used cache entries from cache.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 900 $
 */
public class MRUPolicy extends BaseEvictionPolicy implements EvictionPolicy
{
   private MRUAlgorithm algorithm;


   public MRUPolicy()
   {
      super();
      algorithm = new MRUAlgorithm();
   }

   public EvictionAlgorithm getEvictionAlgorithm()
   {
      return algorithm;
   }

   public Class getEvictionConfigurationClass()
   {
      return MRUConfiguration.class;
   }
}
