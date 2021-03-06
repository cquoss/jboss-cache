/*
 * Configurator.java
 */
package org.jboss.cache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.util.propertyeditor.PropertyEditors;
import org.w3c.dom.*;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;

/**
 * A generic object property configurator. It reads in a user-specified xml file from class path,
 * parses it into xml format, and finally performs reflection to configure
 * the object through setter methods.
 * <p>Here is a typical usage:
 * <pre>
 * MyObject obj = new MyObject();
 * PropertyConfigurator config = new PropertyConfigurator();
 * config.configure(obj, "my.xml");
 * </pre>
 * that will configure MyObject automatically based on the input xml file.
 * <p>Note that this class currently supports configuration parameter as primary types,
 * e.g., int, float, String, long, etc. In addition, it also supports <code>org.w3c.dom.Element</code>
 * type.</p>
 * The tag of the xml file currently suports at the root level "mbean", "attribute" for each
 * property, and, finally, if the attribute type is "Element", the sub-tag name is assumed to be
 * "config".
 * <p>See the tree cache xml file for example.</p>
 *
 * @author bwang
 */
public class PropertyConfigurator {
   private static Log logger_=LogFactory.getLog(PropertyConfigurator.class);
   private Object objToConfigure_;
   private static final String ROOT="mbean";
   private static final String ATTR="attribute";
   private static final String SUB_ATTR="config";
   private static final String NAME="name";

   public PropertyConfigurator() {
      // register property editor for org.w3c.dom.Element
      try {
         Class clasz=Class.forName("org.w3c.dom.Element");
         Class elementClasz=Class.forName("org.jboss.util.propertyeditor.ElementEditor");
         try {
            PropertyEditor editor = PropertyEditors.getEditor(clasz);  // if null, will throw a rte. If not, we will use the existing ones.
            logger_.info("Found existing property editor for org.w3c.dom.Element: " +editor);
         } catch (RuntimeException ex) {
            logger_.info("Property editor for org.w3c.dom.Element does not exist." +
                 " Will register org.jboss.util.propertyeditor.ElementEditor");
            PropertyEditors.registerEditor(clasz, elementClasz);
         }
      }
      catch(ClassNotFoundException ex) {
         logger_.error("Class not found for either org.w3c.dom.Element or " +
                 "org.jboss.util.propertyeditor.ElementEditor");
      }
   }

   protected Element loadDocument(String location) throws ConfigureException {
      URL url=null;
      try {
         url=new URL("file", "", 80, location);  // Use for Main

         logger_.debug("URL location is " + url.toString());
         return loadDocument(url.openStream());
      }
      catch(java.net.MalformedURLException mfx) {
         logger_.error("Configurator error: " + mfx);
         mfx.printStackTrace();
      }
      catch(java.io.IOException e) {
         logger_.error("Configurator error: " + e);
         e.printStackTrace();
      }
      return null;
   }

   protected Element loadDocument(InputStream is) throws ConfigureException {
      Document doc=null;
      try {
         InputSource xmlInp=new InputSource(is);

         DocumentBuilderFactory docBuilderFactory=DocumentBuilderFactory.newInstance();
         DocumentBuilder parser=docBuilderFactory.newDocumentBuilder();
         doc=parser.parse(xmlInp);
         Element root=doc.getDocumentElement();
         root.normalize();
         return root;
      }
      catch(SAXParseException err) {
         logger_.error("Configurator SAXParse error: " + err.getMessage());
         err.printStackTrace();
      }
      catch(SAXException e) {
         logger_.error("Configurator SAX error: " + e);
         e.printStackTrace();
      }
      catch(Exception pce) {
         logger_.error("Configurator general error: " + pce);
         pce.printStackTrace();
      }
      return null;
   }

   protected Element getMBeanElement(Element root) throws ConfigureException {
      // This is following JBoss convention.
      NodeList list=root.getElementsByTagName(ROOT);
      if(list == null)
         throw new ConfigureException("Can't find " + ROOT + " tag");

      if(list.getLength() > 1)
         throw new ConfigureException("Has multiple " + ROOT + " tag");

      Node node=list.item(0);
      Element element=null;
      if(node.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
         element=(Element)node;
      }
      else {
         throw new ConfigureException("Can't find " + ROOT + " element");
      }
      return element;
   }

   /**
    * Configure the object based on the config xml file.
    *
    * @param objToConfigure Object that needs configuration.
    * @param configFile     xml file name that exists in the class path.
    * @throws ConfigureException when the configuration attempt fails.
    */
   public void configure(Object objToConfigure, String configFile) throws ConfigureException {
      // step x. load document
      ClassLoader cl=Thread.currentThread().getContextClassLoader();
      InputStream is=cl.getResourceAsStream(configFile);
      if(is == null) {
         try {
            is=new FileInputStream(configFile);
         }
         catch(FileNotFoundException e) {
            e.printStackTrace();
         }
      }
      if(is == null)
         throw new ConfigureException("could not find resource " + configFile);
      configure(objToConfigure, is);
   }

   /**
    * Configure the object based on the config xml file.
    *
    * @param objToConfigure Object that needs configuration.
    * @param is             InputStream for the configuration xml file.
    * @throws ConfigureException when the configuration attempt fails.
    */
   public void configure(Object objToConfigure, InputStream is) throws ConfigureException {
      objToConfigure_=objToConfigure;

      if(is == null)
         throw new ConfigureException("input stream is null for property xml");

      Element root=loadDocument(is);
      Element mbeanElement=getMBeanElement(root);
      NodeList list=mbeanElement.getElementsByTagName(ATTR);
      logger_.info("attribute size: " + list.getLength());

      Class objClass=objToConfigure_.getClass();
      Method[] methods=objClass.getMethods();
      Class[] string_sig=new Class[]{String.class};

      // step x. loop through attributes
      for(int loop=0; loop < list.getLength(); loop++) {
         Node node=list.item(loop);
         if(node.getNodeType() != org.w3c.dom.Node.ELEMENT_NODE)
            continue;

         Element element=(Element)node;
         String name=element.getAttribute(NAME);
         String valueStr=getElementContent(element, true);
         Element valueObj=null;
         if(valueStr.length() == 0) { // try out element object
            valueObj=getSubElementObject(element);
         }
         String methodName="set" + name;
         Object value=null;
         Method method=null;


         // try whether we have a setter that takes a simple String first
         try {
            method=objClass.getMethod(methodName, string_sig);
         }
         catch(Exception e) {
            ; /* we don't have a setter with a String arg, just continue */
         }

         if(method != null) {
            try {
               logger_.debug("setting attribute " + name + " to " + valueStr);
               method.invoke(objToConfigure_, new Object[]{valueStr});
               continue;
            }
            catch(Exception ex) {
               ex.printStackTrace();
               throw new ConfigureException("configure(): can't invoke " + methodName + " to configure " +
                       "TreeCache properties. Exception: " + ex);
            }
         }


         // step x. Do a reflection on the object class to see
         // if there is a set"Attribute" method. If not, exception is thrown.
         for(int i=0; i < methods.length; i++) {
            // Exclude the possibility of overloading methods now.

            if(methodName.equals(methods[i].getName())) {
               method=methods[i];
               Class[] clz=method.getParameterTypes(); // size should be 1
               if(clz.length != 1)
                  throw new ConfigureException("Parameter size of " + methodName +
                          " is not 1 but " + clz.length);

               Class classParam=clz[0];
               // step x. Get the class type for the set"Attribute" argument and convert
               // the attribute from String to the named class type.
               PropertyEditor editor=PropertyEditorManager.findEditor(classParam);
               if(editor == null) {
                  String str="Could not find PropertyEditor for type class " +
                          classParam;
                  throw new ConfigureException(str);
               }

               if(valueObj != null) { // means there is a sub element
                  editor.setValue(valueObj);
               }
               else {
                  editor.setAsText(valueStr);
               }

               value=editor.getValue();
               logger_.debug("Invoking setter method: " + method +
                       " with parameter \"" + value + "\" of type " + value.getClass());

               // step x. invoke set"Attribute" from method.invoke(object, params)
               try {
                  method.invoke(objToConfigure_, new Object[]{value});
                  break;
               }
               catch(Exception ex) {
                  throw new ConfigureException("can't invoke " + methodName + " to configure TreeCache", ex);
               }
            }
         }
      }
   }

   private Element getSubElementObject(Element element) {
      NodeList nl=element.getChildNodes();
      for(int i=0; i < nl.getLength(); i++) {
         Node node=nl.item(i);
         if(node.getNodeType() == Node.ELEMENT_NODE &&
                 SUB_ATTR.equals( ((Element)node).getTagName() ) ) {
            return (Element)node;
         }
      }

      logger_.debug("getSubElementObject(): element object. Does not exist for " + SUB_ATTR);
      return null;
   }

   private String getElementContent(Element element, boolean trim) {
      NodeList nl=element.getChildNodes();
      String attributeText="";
      for(int i=0; i < nl.getLength(); i++) {
         Node n=nl.item(i);
         if(n instanceof Text) {
            attributeText+=((Text)n).getData();
         }
      } // end of for ()
      if(trim)
         attributeText=attributeText.trim();
      return attributeText;
   }

   public static void main(String[] args) {
      try {
         TreeCache cache=new TreeCache();
         PropertyConfigurator config=new PropertyConfigurator();
         config.configure(cache, "tree-cache.xml");
      }
      catch(Exception ex) {
         ex.printStackTrace();
      }
   }
}
