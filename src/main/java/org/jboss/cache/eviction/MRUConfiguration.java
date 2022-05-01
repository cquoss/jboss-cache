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
 * MRUConfiguration EvictionConfiguration implementation for MRUPolicy.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 1592 $
 */
public class MRUConfiguration implements EvictionConfiguration
{
   private static final Log log = LogFactory.getLog(MRUConfiguration.class);
   private int maxNodes;
   

   public MRUConfiguration()
   {
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
    * Configure the MRU Policy with XML.
    * <p/>
    * This method expects a DOM XML object of something similar to:
    * <p/>
    * <region name="/Test/">
    * <attribute name="maxNodes">10000</attribute>
    * </region>
    *
    * @param element The MRUPolicy configuration XML DOM element.
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
         throw new ConfigureException("Cannot have 0 for maxNodes on a MRUAlgorithm");
      }

      if (log.isDebugEnabled())
      {
         log.debug("parseConfig: name -- " + name + " maxNodes -- "
               + getMaxNodes());
      }
   }

   public String toString()
   {
      StringBuffer str = new StringBuffer();
      str.append("MRUConfiguration: ").
            append(" maxNodes =").append(getMaxNodes());
      return str.toString();
   }
}
