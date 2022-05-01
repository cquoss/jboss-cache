/**
 * 
 */
package org.jboss.cache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class for {@link Node}.
 * @author manik
 */
public abstract class AbstractNode implements DataNode
{
   
   private static Log log = LogFactory.getLog(AbstractNode.class);

   /**
    * Default output indent for printing.
    */
   protected static final int INDENT=4;

   /**
    * Name of the node.
    */
   protected Fqn fqn;

   /**
    * Map of children names to children.
    */
   protected Map children;
  
   /**
    * Map of general data keys to values.
    */
   protected Map data;
   
   /**
    * Returns the name of this node.
    */
   public Object getName()
   {
      return fqn.getLast(); 
   }

   /**
    * Returns the name of this node.
    */
   public Fqn getFqn()
   {
      return fqn;
   }

   public TreeNode getChild(Object child_name)
   {
      if(child_name == null) return null;
      return children == null ? null : (DataNode)children.get(child_name);
   }

   /**
    * Returns null, children may override this method.
    */
   public TreeNode getParent()
   {
      return null;
   }

   public boolean childExists(Object child_name)
   {
      if(child_name == null) return false;
      return children != null && children.containsKey(child_name);
   }

   public Map getChildren()
   {
      return children;
   }

   public void setChildren(Map children)
   {
      this.children = children;
   }

   public boolean hasChildren()
   {
      return children != null && children.size() > 0;
   }

   public void put(Map data)
   {
      put(data, false);
   }

   public void removeChild(Object child_name)
   {
      if(children != null) 
      {
         children.remove(child_name);
         if(log.isTraceEnabled())
            log.trace("removed child " + child_name);
      }
   }

   public void removeAllChildren()
   {
      if(children != null) children.clear();
   }

   public void print(StringBuffer sb, int indent)
   {
      printIndent(sb, indent);
      sb.append(Fqn.SEPARATOR).append(getName());
      if(children != null && children.size() > 0) {
         Collection values=children.values();
         for(Iterator it=values.iterator(); it.hasNext();) {
            sb.append("\n");
            ((DataNode)it.next()).print(sb, indent + INDENT);
         }
      }
   }
   
   public void printIndent(StringBuffer sb, int indent) {
      if(sb != null) {
         for(int i=0; i < indent; i++)
            sb.append(" ");
      }
   }

   public void addChild(Object child_name, TreeNode n)
   {     
      if(child_name != null)
         children().put(child_name, n);
   }

   /** 
    * Returns null or the Map in use with the data.
    * This needs to be called with a lock if concurrency is a concern.
    */
   protected final Map data()
   {
      if(data == null)
         data=new HashMap();
      return data;
   }
   
   /**
    * Override this if concurrent thread access may occur, in which case return
    * a concurrently modifiable Map.
    */
   protected Map children() 
   {
      if(children == null) children=new HashMap();
      return children;
   }
   
   /**
    * Adds details of the node into a map as strings.
    */
   protected void printDetailsInMap(StringBuffer sb, int indent, Map map)
   {
      Map.Entry entry;
      printIndent(sb, indent);
      indent += 2;   // increse it
      sb.append(Fqn.SEPARATOR).append(getName());
      sb.append("\n");
      if(map != null)
      {
         for(Iterator it=map.entrySet().iterator(); it.hasNext();) 
         {
            entry=(Map.Entry)it.next();
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
         }
      }
      if(children != null && children.size() > 0) 
      {
         Collection values=children.values();
         for(Iterator it=values.iterator(); it.hasNext();) 
         {
            sb.append("\n");
            ((DataNode)it.next()).printDetails(sb, indent);
         }
      }
   }

   public abstract Object clone() throws CloneNotSupportedException;

   /** Comment out for now.
   public void relocate(DataNode newParentNode, Fqn newFqn)
   {
      Object child = fqn.get(fqn.size()-1);
      // get rid of parent's children map entry
      getParent().removeChild(child);

      this.fqn = newFqn;
      // reset parent
      parent = newParentNode;
      parent.addChild(child, this);

      Iterator it = children().keySet().iterator();
      while(it.hasNext())
      {
         Object key = it.next();
         Fqn childFqn = new Fqn(fqn, key);
         DataNode node = (DataNode)children.get(key);
         node.relocate(this, childFqn);
      }
   }
   */
}
