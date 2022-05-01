/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import java.io.*;
import java.util.Map;


/**
 * Represents a modification in the cache. Contains the nature of the modification
 * (e.g. PUT, REMOVE), the fqn of the node, the new value and the previous value.
 * A list of modifications will be sent to all nodes in a cluster when a transaction
 * has been committed (PREPARE phase). A Modification is also used to roll back changes,
 * e.g. since we know the previous value, we can reconstruct the previous state by
 * applying the changes in a modification listin reverse order.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 12, 2003
 * @version $Revision: 1574 $
 */
public class Modification implements Externalizable {

   /**
    * Put key/value modification.
    */
   public static final byte PUT_KEY_VALUE=1;

   /**
    * Put data (Map) modification.
    */
   public static final byte PUT_DATA=2;

   /**
    * Erase existing data, put data (Map) modification.
    */
   public static final byte PUT_DATA_ERASE=3;

   /**
    * Remove a node modification.
    */
   public static final byte REMOVE_NODE=4;

   /**
    * Remove a key/value modification.
    */
   public static final byte REMOVE_KEY_VALUE=5;

   /**
    * Clear node's Map modification.
    */
   public static final byte REMOVE_DATA=6;

   /**
    * Stores data in a Fqn using a byte array.
    */
   public static final byte STORE_STATE=7;

   private static final long serialVersionUID = -3074622985730129009L;

   private byte    type=0;
   private Fqn     fqn=null;
   private Object  key=null;
   private Object  value=null;
   private Object  old_value=null;
   private Map     data=null;

   /**
    * Constructs a new modification.
    */
   public Modification() {
   }

   /**
    * Constructs a new modification with details.
    */
   public Modification(byte type, Fqn fqn, Object key, Object value) {
      this.type=type;
      this.fqn=fqn;
      this.key=key;
      this.value=value;
   }

   /**
    * Constructs a new modification with key.
    */
   public Modification(byte type, Fqn fqn, Object key) {
      this.type=type;
      this.fqn=fqn;
      this.key=key;
   }

   /**
    * Constructs a new modification with data map.
    */
   public Modification(byte type, Fqn fqn, Map data) {
      this.type=type;
      this.fqn=fqn;
      this.data=data;
   }

   /**
    * Constructs a new modification with fqn only.
    */
   public Modification(byte type, Fqn fqn) {
      this.type=type;
      this.fqn=fqn;
   }

   /**
    * Returns the type of modification.
    */
   public byte getType() {
      return type;
   }

   /**
    * Sets the type of modification.
    */
   public void setType(byte type) {
      this.type=type;
   }

   /**
    * Returns the modification fqn.
    */
   public Fqn getFqn() {
      return fqn;
   }

   /**
    * Sets the modification fqn.
    */
   public void setFqn(Fqn fqn) {
      this.fqn=fqn;
   }

   /**
    * Returns the modification key.
    */
   public Object getKey() {
      return key;
   }

   /**
    * Sets the modification key.
    */
   public void setKey(Object key) {
      this.key=key;
   }

   /**
    * Returns the modification value.
    */
   public Object getValue() {
      return value;
   }

   /**
    * Sets the modification value.
    */
   public void setValue(Object value) {
      this.value=value;
   }

   /**
    * Returns the <i>post</i> modification old value.
    */
   public Object getOldValue() {
      return old_value;
   }

   /**
    * Sets the <i>post</i> modification old value.
    */
   public void setOldValue(Object old_value) {
      this.old_value=old_value;
   }

   /**
    * Returns the modification Map set.
    */
   public Map getData() {
      return data;
   }

   /**
    * Sets the modification Map set.
    */
   public void setData(Map data) {
      this.data=data;
   }

   /**
    * Writes data to an external stream.
    */
   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeByte(type);

      out.writeBoolean(fqn != null);
      if(fqn != null)
         fqn.writeExternal(out);

      out.writeObject(key);
      out.writeObject(value);
      out.writeObject(old_value);

      out.writeBoolean(data != null);
      if(data != null)
         out.writeObject(data);
   }

   /**
    * Reads data from an external stream.
    */
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      type=in.readByte();

      if(in.readBoolean()) {
         fqn=new Fqn();
         fqn.readExternal(in);
      }

      key=in.readObject();
      value=in.readObject();
      old_value=in.readObject();

      if(in.readBoolean())
         data=(Map)in.readObject();
   }

   /**
    * Returns the type of modification as a string.
    */
   private String typeToString(int type) {
      switch(type) {
         case PUT_KEY_VALUE:
            return "PUT_KEY_VALUE";
         case PUT_DATA:
            return "PUT_DATA";
         case PUT_DATA_ERASE:
            return "PUT_DATA_ERASE";
         case REMOVE_NODE:
            return "REMOVE_NODE";
         case REMOVE_KEY_VALUE:
            return "REMOVE_KEY_VALUE";
         case REMOVE_DATA:
            return "REMOVE_DATA";
         case STORE_STATE:
            return "STORE_STATE";
         default:
            return "<unknown>";
      }
   }

   /**
    * Returns debug information about this modification.
    */
   public String toString() {
      StringBuffer sb=new StringBuffer();
      sb.append(typeToString(type)).append(": ").append(fqn);
      if(key != null)
         sb.append("\nkey=").append(key);
      if(value != null)
         sb.append("\nvalue=").append(value);
      if(old_value != null)
         sb.append("\nold_value=").append(old_value);
      if(data != null)
         sb.append("\ndata=").append(data);
      return sb.toString();
   }

}
