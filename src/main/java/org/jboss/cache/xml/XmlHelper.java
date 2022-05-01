/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.xml;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 *  A simple XML utility class for reading configuration elements
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class XmlHelper
{
    public static String getAttr(Element elem, String myName, String tagName, String attributeName)
    {
        NodeList list = elem.getElementsByTagName(tagName);

        for (int s = 0; s < list.getLength(); s++)
        {
            org.w3c.dom.Node node = list.item(s);
            if (node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
                continue;

            Element element = (Element) node;
            String name = element.getAttribute(attributeName);
            if (name.equals(myName))
            {
                String valueStr = getElementContent(element, true);
                return valueStr;
            }
        }
        return null;
    }

    public static String getElementContent(Element element, boolean trim)
    {
        NodeList nl = element.getChildNodes();
        String attributeText = "";
        for (int i = 0; i < nl.getLength(); i++)
        {
            Node n = nl.item(i);
            if (n instanceof Text)
            {
                attributeText += ((Text) n).getData();
            }
        } // end of for ()
        if (trim)
            attributeText = attributeText.trim();
        return attributeText;
    }

    public static String readStringContents(Element element, String tagName)
    {
        NodeList nodes = element.getElementsByTagName(tagName);
        if (nodes.getLength() > 0)
        {
            Node node = nodes.item(0);
            Element ne = (Element) node;
            NodeList nl2 = ne.getChildNodes();
            Node node2 = nl2.item(0);
            if (node2 != null)
            {
                String value = node2.getNodeValue();
                if (value == null)
                   return "";
                return value.trim();
            }
            else
            {
                return "";
            }
        }
        else
        {
            return "";
        }
    }

   public static String escapeBackslashes(String value) {
      StringBuffer buf = new StringBuffer(value);
      for(int looper = 0; looper < buf.length(); looper++) {
         char curr = buf.charAt(looper);
         char next = 0;
         if(looper + 1 < buf.length())
            next = buf.charAt(looper+1);

         if(curr == '\\') {
            if(next != '\\') {           // only if not already escaped
               buf.insert(looper,'\\');  // escape backslash
            }
            looper++;                    // skip past extra backslash (either the one we added or existing)
         }
      }
      return buf.toString();
   }

    public static Properties readPropertiesContents(Element element, String tagName) throws IOException
    {
        String stringContents = readStringContents(element, tagName);
        if (stringContents == null) return new Properties();
        // JBCACHE-531: escape all backslash characters
        stringContents = escapeBackslashes(stringContents);
        ByteArrayInputStream is = new ByteArrayInputStream(stringContents.trim().getBytes("ISO8859_1"));
        Properties properties = new Properties();
        properties.load(is);
        is.close();
        return properties;
    }

    public static boolean readBooleanContents(Element element, String tagName)
    {
        return readBooleanContents(element, tagName, false);
    }

    public static boolean readBooleanContents(Element element, String tagName, boolean defaultValue)
    {
        String val = readStringContents(element, tagName);
        if (val.equalsIgnoreCase("true") || val.equalsIgnoreCase("false"))
        {
            return Boolean.valueOf(val).booleanValue();
        }
        return defaultValue;
    }

    public static Element stringToElement(String xml) throws Exception
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(xml.getBytes("utf8"));
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document d = builder.parse(bais);
        bais.close();
        return d.getDocumentElement();
    }
}
