/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.collection;

import java.util.List;
import java.util.Collection;
import java.util.Iterator;

/**
 * Abstract class for CachedList
 *
 * @author Scott Marlow
 */

public abstract class CachedListAbstract implements List {
   public void clear()
   {
      // TODO Can use optimization here
      for(int i=size()-1; i >= 0; i--) {
         remove(i);
      }
   }

   public boolean isEmpty()
   {
      return size() == 0;
   }

   public Object[] toArray()
   {
      Object[] objs = new Object[size()];
      for(int i=0; i < size(); i++) {
         objs[i] = get(i);
      }
      return objs;
   }

   public Object[] toArray(Object a[])
   {
      int actualLength = size();
      if(actualLength > a.length)  // need to allocate a larger array
         a = new Object[actualLength];
      int looper;
      for( looper =0; looper < actualLength; looper++) {
         a[looper] = get(looper);
      }
      for( ; looper < a.length;looper ++)
         a[looper] = null; // if the array is larger than needed, set extra slots to null
      return a;
   }

   public boolean add(Object o)
   {
      add(size(), o);
      return true;
   }

   public boolean contains(Object o)
   {
      if (indexOf(o) != -1) return true;
      return false;
   }

   public boolean remove(Object o)
   {
      int i = indexOf(o);
      if(i == -1)
         return false;

      remove(i);
      return true;
   }

   public boolean addAll(int index, Collection c)
   {
      if(c.size() == 0)
         return false;
      Iterator i = c.iterator();
      // should optimize this
      while(i.hasNext()) {
         Object o = i.next();
         add(index++,o);
      }
      return true;
   }

   public boolean addAll(Collection c)
   {
      if(c.size() == 0)
         return false;
      Iterator i = c.iterator();
      while(i.hasNext()) {
         Object o = i.next();
         add(o);
      }
      return true;
   }

   public boolean containsAll(Collection c)
   {
      Iterator iter = c.iterator();
      while (iter.hasNext()) {
         if(!contains(iter.next())) {
            return false;
         }
      }
      return true;
   }

   public boolean removeAll(Collection c)
   {
      Iterator i = c.iterator();
      while(i.hasNext()) {
         Object o = i.next();
         remove(o);
      }
      return true;
   }

   public int hashCode()
   {
      int result = 1;
      for (int i =0; i < size(); i++) {
         Object o = get(i);
         result = 31 * result + (o == null ? 0 : o.hashCode());
      }
      return result;
   }

   public boolean equals(Object object)
   {
      if (object == this)
         return true;
      if (object == null || !(object instanceof List))
         return false;
      List list = (List) object;
      if (size() != list.size())
         return false;
      for (int i=0; i < list.size(); i++) {
         Object value1 = get(i);
         Object value2 = list.get(i);
         if( (value1 == null && value2 != null) ||
                 (value1 != null && !(value1.equals(value2))))
            return false;
      }
      return true;
   }

   public String toString() {
      StringBuffer buf = new StringBuffer();
      int size = size();
      for (int i =0; i < size; i++) {
         Object key = get(i);
         buf.append("[").append(key).append("]");
         if(i <= size) buf.append(", ");
      }

      return buf.toString();
   }

   public boolean retainAll(Collection c)
   {
      boolean changedAnything = false;
      Iterator iter = iterator();
      while(iter.hasNext()) {
         if(! c.contains(iter.next())) {
            iter.remove();
            changedAnything = true;
         }
      }
      return changedAnything;
   }
}
