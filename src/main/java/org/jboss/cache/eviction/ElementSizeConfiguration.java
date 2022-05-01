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
 * @author Daniel Huang
 * @version $Revision: 1852 $
 */
public class ElementSizeConfiguration implements EvictionConfiguration
{
   private static final Log log = LogFactory.getLog(ElementSizeConfiguration.class);
   private int maxElementsPerNode;
   private int maxNodes;

   public int getMaxElementsPerNode()
   {
      return maxElementsPerNode;
   }

   public void setMaxElementsPerNode(int maxElementsPerNode)
   {
      this.maxElementsPerNode = maxElementsPerNode;
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
    * Configure the Element Size Policy with XML.
    * <p/>
    * This method expects the following XML:
    * <p/>
    * <region name="/region/">
    * <attribute name="maxElementsPerNode">100</attribute>
    * <attribute name="maxNodes">10000</attribute>
    * </region>
    *
    * @param element DOM XML containing Element Size Policy configuration.
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

      String maxElementsPerNode = XmlHelper.getAttr(element, EvictionConfiguration.MAX_ELEMENTS_PER_NODE,
            EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
      if (maxElementsPerNode == null)
      {
         throw new ConfigureException("ElementSizeConfiguration.parseXMLConfig(): Null maxElementsPerNode element");
      }
      setMaxElementsPerNode(Integer.parseInt(maxElementsPerNode));

      if (log.isDebugEnabled())
      {
         log.debug("parseConfig: name -- " + name + " maxNodes -- "
               + getMaxNodes() + " maxElementsPerNode -- " + getMaxElementsPerNode());
      }
   }

   public String toString()
   {
      StringBuffer str = new StringBuffer();
      str.append("ElementSizeConfiguration: maxElementsPerNode =");
      str.append(getMaxElementsPerNode()).append(" maxNodes =").append(getMaxNodes());
      return str.toString();
   }
}
