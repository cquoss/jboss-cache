/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.collection;

import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.jboss.cache.DataNode;
import org.jboss.cache.aop.PojoCache;
import org.jboss.cache.aop.util.AopUtil;
import org.jboss.cache.aop.util.Null;

import java.util.*;

/**
 * Map that uses cache as a backend store.
 *
 * @author Ben Wang
 * @author Scott Marlow
 */
public class CachedMapImpl implements Map
{

//   protected static final Log log_ = LogFactory.getLog(CachedMapImpl.class);

   protected PojoCache cache_;
   protected AbstractCollectionInterceptor interceptor_;

   protected CachedMapImpl(PojoCache cache, AbstractCollectionInterceptor interceptor)
   {
      this.cache_ = cache;
      interceptor_ = interceptor;

   }

   protected Fqn getFqn()
   {
      return interceptor_.getFqn();
   }

   // implementation of the java.util.Map interface

   /*
   protected DataNode getNode()
   {
      try {
         return AopUtil.get(cache_, getFqn());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
   */

   protected Set getNodeChildren()
   {
      try {
         return AopUtil.getNodeChildren(cache_, getFqn());
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public Object get(Object key)
   {
      try {
         return Null.toNullValue(cache_.getObject(AopUtil.constructFqn(getFqn(), Null.toNullKeyObject(key))));
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public Object put(Object key, Object value)
   {
      try {
         return cache_.putObject(AopUtil.constructFqn(getFqn(), Null.toNullKeyObject(key)), Null.toNullObject(value));
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void putAll(Map map)
   {
      for (Iterator i = map.entrySet().iterator(); i.hasNext();) {
         Map.Entry entry = (Map.Entry) i.next();
         put(entry.getKey(), entry.getValue());
      }
   }

   public Object remove(Object key)
   {
      try {
         return cache_.removeObject(AopUtil.constructFqn(getFqn(), Null.toNullKeyObject(key)));
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void clear()
   {
      // Need to clone first to avoid CME
      ArrayList list = new ArrayList(keySet());
      for(int i=0; i < list.size(); i++) {
         remove(list.get(i));
      }
   }

   public int size()
   {
      Set children = getNodeChildren();
      return children == null ? 0 : children.size();
   }

   public boolean isEmpty()
   {
      return size() == 0;
   }

   public boolean containsKey(Object object)
   {
      Set children = getNodeChildren();
      if(object != null)               // if not null,
         object = object.toString();   // convert to internal form which is always string
      return children != null && children.contains(Null.toNullKeyObject(object));
   }

   public boolean containsValue(Object object)
   {
      return values().contains(Null.toNullObject(object));
   }

   public Set entrySet()
   {
      final CachedMapImpl map = this;

      return new AbstractSet()
      {

         public int size()
         {
            Set children = getNodeChildren();
            return children == null ? 0 : children.size();
         }

         public Iterator iterator()
         {
            Set children = getNodeChildren();
            final Iterator i =
                  children == null
                  ? Collections.EMPTY_LIST.iterator()
                  : children.iterator();
            return new Iterator()
            {
               Object lastKey; // for remove

               public boolean hasNext()
               {
                  return i.hasNext();
               }

               public Object next()
               {
                  return new Entry(lastKey = i.next());
               }

               public void remove()
               {
                  map.remove(lastKey);
               }
            };
         }
      };
   }

   public Collection values()
   {
      final CachedMapImpl map = this;

      return new AbstractCollection()
      {

         public int size()
         {
            Set children = getNodeChildren();
            return children == null ? 0 : children.size();
         }

         public void clear() {
            map.clear();
         }

         public Iterator iterator()
         {
            Set children = getNodeChildren();
            final Iterator i =
                  children == null
                  ? Collections.EMPTY_LIST.iterator()
                  : children.iterator();

            return new Iterator()
            {
               Object lastKey; // for remove

               public boolean hasNext()
               {
                  return i.hasNext();
               }

               public Object next()
               {
                  try
                  {
                     lastKey = i.next();
                     return Null.toNullValue(cache_.getObject(AopUtil.constructFqn(getFqn(), lastKey)));

               } catch (RuntimeException re) {
                  throw re;
                  }
                  catch (CacheException e)
                  {
                     throw new RuntimeException(e);
                  }
               }

               public void remove()
               {
                  Object key = lastKey;
                  if(key != null)  // convert from internal Null form to actual null if needed
                     key = Null.toNullKeyValue(key);
                  map.remove(key);
               }
            };
         }
      };
   }

   public Set keySet()
   {
      final CachedMapImpl map = this;

      return new AbstractSet() {

         public int size()
         {
            Set children = getNodeChildren();
            return children == null ? 0 : children.size();
         }

         public Iterator iterator()
         {
            Set children = getNodeChildren();
            final Iterator i =
                  children == null
                  ? Collections.EMPTY_LIST.iterator()
                  : children.iterator();

            return new Iterator()
            {
               Object lastKey; // for remove

               public boolean hasNext()
               {
                  return i.hasNext();
               }

               public Object next()
               {
                  lastKey = i.next();
                  return Null.toNullKeyValue(lastKey);

               }

               public void remove()
               {
                  Object key = lastKey;
                  if(key != null)  // convert from internal Null form to actual null if needed
                     key = Null.toNullKeyValue(key);
                  map.remove(key);
               }
            };

         };
      };
   }

   public int hashCode()
   {
      int result = 0;
      for (Iterator i = entrySet().iterator(); i.hasNext();) {
         result += i.next().hashCode();
      }
      return result;
   }

   public boolean equals(Object object)
   {
      if (object == this)
         return true;
      if (object == null || !(object instanceof Map))
         return false;
      Map map = (Map) object;
      if (size() != map.size())
         return false;
      for (Iterator i = entrySet().iterator(); i.hasNext();) {
         Entry entry = (Entry) i.next();
         Object value = entry.getValue();
         Object key = entry.getKey();
         if (value == null) {
            if(! (map.get(key) == null && map.containsKey(key))) {
               return false;
            }
         }
         else {
            if (! value.equals(map.get(key)))
                return false;
         }
      }
      return true;
   }

   public String toString() {
      StringBuffer buf = new StringBuffer();
      Set set = keySet();
      for(Iterator it = set.iterator(); it.hasNext();) {
         Object key = it.next();
         buf.append("[").append(key).append(", ").append(get(key)).append("]");
         if(it.hasNext()) buf.append(", ");
      }

      return buf.toString();
   }

   protected class Entry implements Map.Entry
   {

      Object key;

      public Entry(Object key)
      {
         this.key = key;
      }

      public Object getKey()
      {
         return Null.toNullValue(key);
      }

      public Object getValue()
      {
         try {
            return Null.toNullValue(cache_.getObject(AopUtil.constructFqn(getFqn(), key)));
         } catch (RuntimeException re) {
            throw re;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }

      public Object setValue(Object value)
      {
         try {
            return cache_.putObject(AopUtil.constructFqn(getFqn(), key), Null.toNullObject(value));
         } catch (RuntimeException re) {
            throw re;                     
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }

      public int hashCode()
      {
         Object value = getValue();
         return ((key == null) ? 0 : key.hashCode())
               ^ ((value == null) ? 0 : value.hashCode());
      }

      public boolean equals(Object obj)
      {
         if (!(obj instanceof Entry))
            return false;
         Entry entry = (Entry) obj;
         Object value = getValue();
         return (
               key == null
               ? entry.getKey() == null
               : key.equals(entry.getKey()))
               && (value == null
               ? entry.getValue() == null
               : value.equals(entry.getValue()));
      }
   }


}
