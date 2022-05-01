/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.ConfigureException;
import org.jboss.cache.xml.XmlHelper;
import org.w3c.dom.Element;

/**
 * LRU Configuration implementation.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 900 $
 */
public class LRUConfiguration implements EvictionConfiguration
{
   private static final Log log = LogFactory.getLog(LRUConfiguration.class);

   private int maxNodes;
   private int timeToLiveSeconds;
   private int maxAgeSeconds;

   public int getMaxNodes()
   {
      return maxNodes;
   }

   public void setMaxNodes(int maxNodes)
   {
      this.maxNodes = maxNodes;
   }

   public int getTimeToLiveSeconds()
   {
      return timeToLiveSeconds;
   }

   public void setTimeToLiveSeconds(int timeToLiveSeconds)
   {
      this.timeToLiveSeconds = timeToLiveSeconds;
   }

   public int getMaxAgeSeconds()
   {
      return maxAgeSeconds;
   }

   public void setMaxAgeSeconds(int maxAgeSeconds)
   {
      this.maxAgeSeconds = maxAgeSeconds;
   }

   /**
    * Configure the LRU Policy with XML.
    * <p/>
    * This method expects the following XML:
    * <p/>
    * <region name="/maxAgeTest/">
    * <attribute name="maxNodes">10000</attribute>
    * <attribute name="timeToLiveSeconds">8</attribute>
    * <attribute name="maxAgeSeconds">10</attribute>
    * </region>
    *
    * @param element DOM XML containing LFU Policy configuration.
    * @throws ConfigureException
    */
   public void parseXMLConfig(Element element) throws ConfigureException
   {
      String name = element.getAttribute(EvictionConfiguration.NAME);
      if (name == null || name.equals(""))
      {
         throw new ConfigureException("Name is required for the eviction region");
      }

      String maxNodes = XmlHelper.getAttr(element, EvictionConfiguration.MAX_NODES,
            EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
      if (maxNodes != null && !maxNodes.equals(""))
      {
         setMaxNodes(Integer.parseInt(maxNodes));
      }
      else
      {
         setMaxNodes(0);
      }
      String timeToLive = XmlHelper.getAttr(element, EvictionConfiguration.TIME_TO_IDLE_SECONDS,
            EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
      if (timeToLive == null)
      {
         timeToLive = XmlHelper.getAttr(element, EvictionConfiguration.TIME_TO_LIVE_SECONDS,
               EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
         if (timeToLive == null)
            throw new ConfigureException("LRUConfiguration.parseXMLConfig(): Null timeToLiveSeconds element");
      }
      setTimeToLiveSeconds(Integer.parseInt(timeToLive));

      String maxAge = XmlHelper.getAttr(element, EvictionConfiguration.MAX_AGE_SECONDS,
            EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
      if (maxAge != null && !maxAge.equals(""))
      {
         setMaxAgeSeconds(Integer.parseInt(maxAge));
      }

      if (log.isDebugEnabled())
      {
         log.debug("parseConfig: name -- " + name + " maxNodes -- "
               + getMaxNodes() + " timeToLiveSeconds -- " + getTimeToLiveSeconds() + " maxAgeSeconds -- "
               + getMaxAgeSeconds());
      }
   }

   public String toString()
   {
      StringBuffer str = new StringBuffer();
      str.append("LRUConfiguration: timeToLiveSeconds = ").append(getTimeToLiveSeconds()).append(" maxAgeSeconds =");
      str.append(getMaxAgeSeconds()).append(" maxNodes =").append(getMaxNodes());
      return str.toString();
   }
}
