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
 * LFU Configuration implementation for LFU Policy.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 1852 $
 */
public class LFUConfiguration implements EvictionConfiguration
{
   private static final Log log = LogFactory.getLog(LFUConfiguration.class);

   private int minNodes;
   private int maxNodes;

   public int getMinNodes()
   {
      return minNodes;
   }

   public void setMinNodes(int minNodes)
   {
      this.minNodes = minNodes;
   }

   public int getMaxNodes()
   {
      return maxNodes;
   }

   public void setMaxNodes(int maxNodes)
   {
      this.maxNodes = maxNodes;
   }

   /**
    * Configures the LFU Policy with XML.
    * <p/>
    * This method expects the following XML DOM:
    * <p/>
    * <region name="abc">
    * <attribute name="minNodes">10</attribute>
    * <attribute name="maxNodes">20</attribute>
    * </region>
    *
    * @param element XML DOM object containing LFU configuration.
    * @throws ConfigureException
    */
   public void parseXMLConfig(Element element) throws ConfigureException
   {
      String name = element.getAttribute(EvictionConfiguration.NAME);
      if (name == null || name.equals(""))
      {
         throw new ConfigureException("Name is required for the eviction region");
      }

      String maxNodes = XmlHelper.getAttr(element,
            EvictionConfiguration.MAX_NODES, EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
      if (maxNodes != null)
      {
         setMaxNodes(Integer.parseInt(maxNodes));
      }
      else
      {
         setMaxNodes(0);
      }

      String minNodes = XmlHelper.getAttr(element, EvictionConfiguration.MIN_NODES,
            EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
      if (minNodes != null)
      {
         setMinNodes(Integer.parseInt(minNodes));
      }
      else
      {
         setMinNodes(0);
      }

      if (log.isDebugEnabled())
      {
         log.debug("parseConfig: name -- " + name + " maxNodes -- "
               + getMaxNodes() + " minNodes -- " + getMinNodes());
      }
   }

   public String toString()
   {
      StringBuffer ret = new StringBuffer();
      ret.append("LFUConfiguration: maxNodes = ").append(getMaxNodes()).append(" minNodes = ").append(getMinNodes());
      return ret.toString();
   }
}
