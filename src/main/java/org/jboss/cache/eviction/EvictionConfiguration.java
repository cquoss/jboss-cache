/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.jboss.cache.ConfigureException;
import org.w3c.dom.Element;

/**
 * Eviction Configuration Interface.
 * <p/>
 * This object serves as a POJO/Bean for getting and setting cache properties.
 * <p/>
 * It also encapsulates XML parsing and configuration of a given type of EvictionPolicy.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 1852 $
 */
public interface EvictionConfiguration
{
   public static final int WAKEUP_DEFAULT = 5;

   public static final String ATTR = "attribute";
   public static final String NAME = "name";

   public static final String REGION = "region";
   public static final String WAKEUP_INTERVAL_SECONDS = "wakeUpIntervalSeconds";
   public static final String MAX_NODES = "maxNodes";
   public static final String MAX_ELEMENTS_PER_NODE = "maxElementsPerNode";
   public static final String TIME_TO_IDLE_SECONDS = "timeToIdleSeconds";
   public static final String TIME_TO_LIVE_SECONDS = "timeToLiveSeconds";
   public static final String MAX_AGE_SECONDS = "maxAgeSeconds";
   public static final String MIN_NODES = "minNodes";
   public static final String MIN_ELEMENTS = "minElements";
   public static final String REGION_POLICY_CLASS = "policyClass";

   public static final int NODE_GRANULARITY = 0;
   public static final int ELEMENT_GRANULARITY = 1;

   /**
    * Parse the XML configuration for the given specific eviction region.
    * <p/>
    * The element parameter should contain the entire region block. An example
    * of an entire Element of the region would be:
    * <p/>
    * <region name="abc">
    * <attribute name="maxNodes">10</attribute>
    * </region>
    *
    * @param element DOM element for the region. <region name="abc"></region>
    * @throws ConfigureException
    */
   public void parseXMLConfig(Element element) throws ConfigureException;
}
