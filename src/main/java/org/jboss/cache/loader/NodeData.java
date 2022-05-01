package org.jboss.cache.loader;

import org.jboss.cache.Fqn;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Serializable representation of the data of a node (FQN and attributes)
 * @author Bela Ban
 * @version $Id: NodeData.java 662 2005-10-08 00:40:25Z bstansberry $
 */
public class NodeData implements Externalizable {
   Fqn     fqn=null;
   Map     attrs=null;

   static final long serialVersionUID = -7571995794010294485L;

   public NodeData() {
   }

   public NodeData(Fqn fqn) {
      this.fqn=fqn;
   }

   public NodeData(Fqn fqn, Map attrs) {
      this.fqn=fqn;
      this.attrs=attrs;
   }

   public NodeData(String fqn, Map attrs) {
         this.fqn=Fqn.fromString(fqn);
         this.attrs=attrs;
      }
   
   public Map getAttributes() {
      return attrs;
   }
   
   public Fqn getFqn() {
      return fqn;
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(fqn);
      if(attrs != null) {
         out.writeBoolean(true);
         out.writeObject(attrs);
      }
      else {
         out.writeBoolean(false);
      }
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      fqn=(Fqn)in.readObject();
      if(in.readBoolean())
         attrs=(Map)in.readObject();
   }

   public String toString() {
      return "fqn: " + fqn + ", attrs=" + attrs;
   }

}
