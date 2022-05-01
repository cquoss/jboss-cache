/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.collection;

import org.jboss.cache.CacheException;
import org.jboss.cache.DataNode;
import org.jboss.cache.Fqn;
import org.jboss.cache.aop.PojoCache;
import org.jboss.cache.aop.util.AopUtil;
import org.jboss.cache.aop.util.Null;

import java.util.*;

/**
 * Set that uses cache as a underlying backend store
 *
 * @author Ben Wang
 * @author Scott Marlow
 * @author Jussi Pyörre
 */
public class CachedSetImpl extends AbstractSet
{
//   protected static final Log log_=LogFactory.getLog(CachedSetImpl.class);

   protected PojoCache cache_;
   protected AbstractCollectionInterceptor interceptor_;

   public CachedSetImpl(PojoCache cache, AbstractCollectionInterceptor interceptor)
   {
      this.cache_ = cache;
      this.interceptor_ = interceptor;
   }

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

   protected Fqn getFqn()
   {
      // Need this since fqn can be reset.
      return interceptor_.getFqn();
   }

   // implementation of the java.util.Set interface
   public int size()
   {
      return keySet().size();
   }

   public Iterator iterator()
   {
      return new IteratorImpl(keySet());
   }


   public boolean add(Object o)
   {
      Collection keys = keySet();

      // This could be done with 'contains(o)' but it would invoke 'keySet()'
      // twice
      for (Iterator iter = new IteratorImpl(keys); iter.hasNext();)
         if (iter.next() == o)
            return false;

      // Search for an available key. This is a fast operation as the key set
      // is already available. Start with the size of the key set and go
      // up as this will be the fastest way to find an unused key. Gaps
      // in the keys don't matter, as this is a Set not a List
      int key = keys.size();
      String keyString;
      while (keys.contains((keyString = Integer.toString(key))))
         key++;

      try {
         cache_.putObject(AopUtil.constructFqn(getFqn(), keyString), Null.toNullObject(o));
         return true;
      } catch (RuntimeException re) {
         throw re;
      } catch (CacheException e) {
         throw new RuntimeException(e);
      }
   }

   public boolean contains(Object o) {
      Iterator iter = iterator();
      if (o==null) {
         while (iter.hasNext()) {
            if (iter.next()==null) {
               return true;
            }
         }
     } else {
         while (iter.hasNext()) {
            if (o.equals(iter.next())) {
             return true;
            }
         }
     }
     return false;
     }


   public String toString() {
      StringBuffer buf = new StringBuffer();
      for(Iterator it = iterator(); it.hasNext();) {
         Object key = it.next();
         buf.append("[").append(key).append("]");
         if(it.hasNext()) buf.append(", ");
      }

      return buf.toString();
   }

   public boolean equals(Object o)
   {
      if (o == null)
         return false;

      if (o == this)
         return true;

      try {
         Set set = (Set) o;

         return (set.size() == keySet().size() && this.containsAll(set));
      } catch (ClassCastException e) {
         return false;
      }
      catch (NullPointerException unused) {
         return false;
      }
   }

   private Collection keySet()
   {
      Set children = getNodeChildren();
      return (children == null)? Collections.EMPTY_SET : children;
   }

   private class IteratorImpl implements Iterator
   {
      private Iterator iterator;

      private Object key;

      private IteratorImpl(Collection keys)
      {
         iterator = keys.iterator();
      }

      public boolean hasNext()
      {
         return iterator.hasNext();
      }

      public Object next()
      {
         // (Brian) Removed Jussi's call to iterator.hasNext() followed by
         // an NSOE if false.  That approach was slightly more efficient if
         // hasNext() were false, but in the vast majority of cases iterators
         // are used correctly and it was an extra step.

         this.key = iterator.next();

         try {
            return Null.toNullValue(cache_.getObject(AopUtil.constructFqn(getFqn(), this.key)));
         } catch (RuntimeException re) {
            throw re;
         } catch (CacheException e) {
            throw new RuntimeException(e);
         }
      }

      public void remove() throws IllegalStateException {
         if (this.key == null) {
            throw new IllegalStateException();
         }

         try {
            cache_.removeObject(AopUtil.constructFqn(getFqn(), this.key));
         } catch (RuntimeException re) {
            throw re;                     
         } catch (CacheException e) {
            throw new RuntimeException(e);
         }
      }

   }
}