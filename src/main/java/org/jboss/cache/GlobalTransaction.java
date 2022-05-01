/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import org.jgroups.Address;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Uniquely identifies a transaction that spans all nodes in a cluster. This is used when
 * replicating all modifications in a transaction; the PREPARE and COMMIT (or ROLLBACK)
 * messages have to have a unique identifier to associate the changes with<br>
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 12, 2003
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @version $Revision: 3104 $
 */
public class GlobalTransaction implements Externalizable {
   Address addr=null;
   long id=-1;
   private static long sid=0;
   private transient boolean remote=false;

   private static final long serialVersionUID = 8011434781266976149L;

   // cash the hashcode
   private transient int hash_code=-1;  // in the worst case, hashCode() returns 0, then increases, so we're safe here

    /**
     * empty ctor used by externalization
     */
   public GlobalTransaction()
   {
   }

   private GlobalTransaction(Address addr)
   {
      this.addr=addr;
      id=newId();
   }

   private static synchronized long newId() {
      return ++sid;
   }

   public static GlobalTransaction create(Address addr)
   {
      return new GlobalTransaction(addr);
   }

   public Object getAddress() {
      return addr;
   }
   
   public void setAddress(Address address)
   {
      addr = address;
   }

   public long getId() {
      return id;
   }

   public int hashCode() {
      if(hash_code == -1)
      {
         hash_code=(addr != null ? addr.hashCode() : 0) + (int)id;
      }
      return hash_code;
   }

   public boolean equals(Object other)
   {
      if (this == other)
         return true;
      if (!(other instanceof GlobalTransaction))
         return false;

      GlobalTransaction otherGtx = (GlobalTransaction) other;

      return ((addr == null && otherGtx.addr == null) || (addr != null && otherGtx.addr != null && addr.compareTo(otherGtx.addr) == 0)) &&
                id == otherGtx.id;
   }


   public String toString() {
      StringBuffer sb=new StringBuffer();
      sb.append("GlobalTransaction:<").append(addr).append(">:").append(id);
      return sb.toString();
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(addr);
      out.writeLong(id);
      // out.writeInt(hash_code);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      addr=(Address)in.readObject();
      id=in.readLong();
      // hash_code=in.readInt();
   }

   /**
    * @return Returns the remote.
    */
   public boolean isRemote()
   {
      return remote;
   }

   /**
    * @param remote The remote to set.
    */
   public void setRemote(boolean remote)
   {
      this.remote = remote;
   }


    public void setId(long id)
    {
        this.id = id;
    }
}
