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
 * FIFO Configuration for FIFO Eviction Policy.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 1852 $
 */
public class FIFOConfiguration implements EvictionConfiguration
{
   private static final Log log = LogFactory.getLog(FIFOConfiguration.class);
   private int maxNodes;

   /**
    * Get the maximum number of nodes or elements configured for the FIFOPolicy.
    *
    * @return maxNodes
    */
   public int getMaxNodes()
   {
      return maxNodes;
   }

   public void setMaxNodes(int maxNodes)
   {
      this.maxNodes = maxNodes;
   }

   /**
    * Configure the FIFOPolicy using XML.
    * <p/>
    * This method expects a DOM object in the following format specific to FIFO:
    * <p/>
    * <region name="abc">
    * <attribute name="maxNodes">1000</attribute>
    * </region>
    * <p/>
    * FIFO requires a "maxNodes" attribute otherwise a ConfigureException is thrown.
    *
    * @param element XML DOM element containing the proper FIFO configuration XML.
    * @throws ConfigureException
    */
   public void parseXMLConfig(Element element) throws ConfigureException
   {
      String name = element.getAttribute(NAME);

      if (name == null || name.equals(""))
      {
         throw new ConfigureException("Name is required for the eviction region");
      }

      String maxNodes = XmlHelper.getAttr(element,
            MAX_NODES, EvictionConfiguration.ATTR, EvictionConfiguration.NAME);
      if (maxNodes != null && !maxNodes.equals(""))
      {
         setMaxNodes(Integer.parseInt(maxNodes));

      }
      else
      {
         throw new ConfigureException("FIFOConfiguration requires maxNodes attribute");
      }

      if (log.isDebugEnabled())
      {
         log.debug("parseConfig: name -- " + name + " maxNodes -- "
               + getMaxNodes());
      }
   }

   public String toString()
   {
      StringBuffer ret = new StringBuffer();
      ret.append("LFUConfiguration: maxNodes = ").append(getMaxNodes());
      return ret.toString();
   }
}
