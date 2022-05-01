/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.collection;

import org.jboss.cache.Fqn;
import org.jboss.cache.DataNode;
import org.jboss.cache.aop.PojoCache;
import org.jboss.cache.aop.util.AopUtil;
import org.jboss.cache.aop.util.Null;

import java.util.*;

/**
 * List implementation that uses cache as a backend store.
 *
 * @author Ben Wang
 * @author Scott Marlow
 */

public class CachedListImpl extends CachedListAbstract implements List
{

//   protected static final Log log_ = LogFactory.getLog(CachedListImpl.class);
   protected PojoCache cache_;
   protected AbstractCollectionInterceptor interceptor_;

   public CachedListImpl(PojoCache cache, AbstractCollectionInterceptor interceptor)
   {
      cache_ = cache;
      interceptor_ = interceptor;
   }

   protected Fqn getFqn()
   {
      return interceptor_.getFqn();
   }

   // implementation of the java.util.List interface
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

   public Object get(int index)
   {
      checkIndex(index);
      try {
         return Null.toNullValue(cache_.getObject(AopUtil.constructFqn(getFqn(), Integer.toString(index))));
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private void checkIndex(int i) {
      // TODO This is too expensive now to check it everytime from the cache (potentially twice).
      // It is showing up in the JProfiler. So I am disabling it now.
      return;
/*
      if(size() == 0) return; // No need to check here.
      if( i < 0 || i >= size() ) {
         throw new IndexOutOfBoundsException("Index out of bound at CachedListImpl(). Index is " +i
         + " but size is " +size());
      } */
   }

   public int size()
   {
      Set children = getNodeChildren();
      return children == null ? 0 : children.size();
   }

   public Object set(int index, Object element)
   {
      try {
         if(index != 0)
            checkIndex(index-1); // Since index can be size().
         return Null.toNullValue(cache_.putObject(AopUtil.constructFqn(getFqn(), Integer.toString(index)), Null.toNullObject(element)));
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void add(int index, Object element)
   {
      try {
         if(index != 0)
            checkIndex(index-1); // Since index can be size().
         for (int i = size(); i > index; i--) {
            Object obj = cache_.removeObject(AopUtil.constructFqn(getFqn(), Integer.toString(i - 1)));
            cache_.putObject(AopUtil.constructFqn(getFqn(), Integer.toString(i)), obj);
         }
         cache_.putObject(AopUtil.constructFqn(getFqn(), Integer.toString(index)), Null.toNullObject(element));
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public int indexOf(Object o)
   {
      int size = size();
      if(o == null) {
         for(int i=0; i < size; i++) {
            if(null == get(i))
               return i;
         }
      }
      else {
         for(int i=0; i < size; i++) {
            if(o.equals(get(i)))
               return i;
         }
      }
      return -1;
   }

   public int lastIndexOf(Object o)
   {
      if(o == null) {
         for(int i=size() - 1 ; i >=0 ; i--) {
            if(null == get(i))
               return i;
         }
      }
      else {
         for(int i=size() - 1 ; i >=0 ; i--) {
            if(o.equals(get(i)))
               return i;
         }
      }
      return -1;
   }

   public Object remove(int index)
   {
      try {
         checkIndex(index);
         // Object result = cache.removeObject(((Fqn) fqn.clone()).add(new Integer(index)));
         int size = size();
         Object result = Null.toNullValue(cache_.removeObject(AopUtil.constructFqn(getFqn(), Integer.toString(index))));
         if( size == (index +1)) {
            return result; // We are the last one.
         }
         for (int i = index; i < size-1; i++) {
            Object obj = cache_.removeObject(AopUtil.constructFqn(getFqn(), Integer.toString(i + 1)));
            cache_.putObject(AopUtil.constructFqn(getFqn(), Integer.toString(i)), obj);
         }
         return result;
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public Iterator iterator()
   {
      // TODO: check for concurrent modification
      return new Iterator()
      {
         // Need to share this
         protected int current = -1;
         protected int size = size();

         public boolean hasNext()
         {
            if(size ==0) return false;
            if(current > size)
               throw new NoSuchElementException("CachedSetImpl.iterator.hasNext(). " +
                       " Cursor position " + current + " is greater than the size " +size());

            return current < size-1;
         }

         public Object next()
         {
            if(current == size)
               throw new NoSuchElementException("CachedSetImpl.iterator.next(). " +
                       " Cursor position " + current + " is greater than the size " +size());

            try {
               return Null.toNullValue(cache_.getObject(AopUtil.constructFqn(getFqn(), Integer.toString(++current))));
            } catch (RuntimeException re) {
               throw re;
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }

         public void remove()
         {
            // TODO Need optimization here since set does not care about index
            try {
               if(size ==0) return;
               if(current == size)
                  throw new IllegalStateException("CachedSetImpl.iterator.remove(). " +
                          " Cursor position " + current + " is greater than the size " +size);
               if (current < (size-1)) {
                  // Need to reshuffle the items.
                  Object last = cache_.removeObject(AopUtil.constructFqn(getFqn(), Integer.toString(current)));
                  for(int i = current+1 ; i < size; i++) {
                     last = cache_.removeObject(AopUtil.constructFqn(getFqn(), Integer.toString(i)));
                     cache_.putObject(AopUtil.constructFqn(getFqn(), Integer.toString(i-1)), last);
                  }
               } else { // we are the last index.
                  // Need to move back the cursor.
                  cache_.removeObject(AopUtil.constructFqn(getFqn(), Integer.toString(current)));
               }
               current--;
               size--;
            } catch (RuntimeException re) {
               throw re;
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };
   }

   public List subList(int fromIndex, int toIndex)
   {
      if( fromIndex < 0)
         throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
      if( toIndex > size())
         throw new IndexOutOfBoundsException("toIndex = " + toIndex + " but size() =" + size());
      if( fromIndex > toIndex)
         throw new IllegalArgumentException("fromIndex ("+fromIndex+") must be less than toIndex("+toIndex+")");
      if(fromIndex == toIndex)            // request for empty list?
         return new LinkedList();
      return new MyCachedSubListImpl(this, fromIndex, toIndex);
   }

   public ListIterator listIterator()
   {
      return new MyListIterator(this,0);
   }

   public ListIterator listIterator(int index)
   {
      return new MyListIterator(this, index);
   }

   protected static class MyListIterator implements ListIterator {
      protected int index = 0;
      protected List list_;

      public MyListIterator(List list, int index) {
         list_ = list;
         if(index < 0 || index > list_.size()) {
            throw new IndexOutOfBoundsException("CachedListImpl: MyListIterator construction. " +
                  " Index is out of bound : " +index);
         }
         this.index = index;
      }

      public int nextIndex()
      {
         return index;
      }

      public int previousIndex()
      {
         return index-1;
      }

      public void remove()
      {

         try {
            int size = list_.size();
            if(size == 0) return;
            if(previousIndex() == size)
               throw new IllegalStateException("CachedSetImpl.MyListIterator.remove(). " +
                       " Cursor position " + index + " is greater than the size " +size);
            if (previousIndex() < (size)) {
               list_.remove(previousIndex());
               index--;
            }
         } catch (RuntimeException re) {
            throw re;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }

      }

      public boolean hasNext()
      {
         return (index < list_.size());
      }

      public boolean hasPrevious()
      {
         return (index != 0);
      }

      public Object next()
      {
         if( index == list_.size() )
            throw new NoSuchElementException();

         index++;
         return list_.get(index-1);  // pass zero relative index
      }

      public Object previous()
      {
         if( index == 0 )
            throw new NoSuchElementException();

         index--;
         return list_.get(index);
      }

      public void add(Object o)
      {
         int size = list_.size();
         if(size == 0) return;

         if(previousIndex() == size)
            throw new IllegalStateException("CachedSetImpl.MyListIterator.add(). " +
                    " Cursor position " + index + " is greater than the size " +size);
         if (previousIndex() < (size)) {
            list_.add(previousIndex(), o);
         }
      }

      public void set(Object o)
      {
         int size = list_.size();
         if(size == 0) return;

         if(previousIndex() == size)
            throw new IllegalStateException("CachedSetImpl.MyListIterator.set(). " +
                    " Cursor position " + index + " is greater than the size " +size);
         if (previousIndex() < (size)) {
            list_.set(previousIndex(), o);
         }
      }
   }

   static public class MyCachedSubListImpl extends CachedListAbstract implements List {

      private List backStore_;
      private int fromIndex_;
      private int toIndex_;

      MyCachedSubListImpl(List backStore, int fromIndex, int toIndex) {
         backStore_ = backStore;
         fromIndex_ = fromIndex;
         toIndex_ = toIndex;
      }

      public int size()
      {
         int size = backStore_.size();
         if(size > toIndex_)
            size = toIndex_;
         size -= fromIndex_;     // subtract number of items ignored at the start of list
         return size;
      }

      public Iterator iterator()
      {
         // TODO: check for concurrent modification
         return new Iterator()
         {
            protected int current = -1;
            protected Iterator iter_ = initializeIter();

            private Iterator initializeIter() {
               Iterator iter = backStore_.iterator();
               for(int looper = 0; looper < fromIndex_;looper ++)
                  if(iter.hasNext())      // skip past to where we need to start from
                     iter.next();
               return iter;
            }

            public boolean hasNext()
            {
               int size = size();
               if(size ==0) return false;
               if(current > size)
                  throw new IllegalStateException("CachedSetImpl.MyCachedSubListImpl.iterator.hasNext(). " +
                          " Cursor position " + current + " is greater than the size " +size());

               return current < size()-1;
            }

            public Object next()
            {
               if(current == size())
                  throw new IllegalStateException("CachedSetImpl.MyCachedSubListImpl.iterator.next(). " +
                          " Cursor position " + current + " is greater than the size " +size());
               current++;
               try {
                  return iter_.next();
               } catch (RuntimeException re) {
                  throw re;
               } catch (Exception e) {
                  throw new RuntimeException(e);
               }
            }

            public void remove()
            {
               iter_.remove();
               current--;
            }
         };

      }

      public Object get(int index)
      {
         checkIndex(index);
         return backStore_.get(index + fromIndex_);
      }

      public Object set(int index, Object element)
      {
         checkIndex(index);
         return backStore_.set(index + fromIndex_, element);
      }

      public void add(int index, Object element)
      {
         backStore_.add(index + fromIndex_, element);
      }

      public Object remove(int index)
      {
         return backStore_.remove(index + fromIndex_);
      }

      public int indexOf(Object o)
      {
         int index = backStore_.indexOf(o);
         if(index < fromIndex_ || index >= toIndex_)
            index = -1;
         else
            index -= fromIndex_;    // convert to be relative to our from/to range
         return index;
      }

      public int lastIndexOf(Object o)
      {
         int index = backStore_.lastIndexOf(o);
         if(index < fromIndex_ || index >= toIndex_)
            index = -1;
         else
            index -= fromIndex_;    // convert to be relative to our from/to range
         return index;
      }

      public ListIterator listIterator()
      {
         return new MyListIterator(this,0);
      }

      public ListIterator listIterator(int index)
      {
         return new MyListIterator(this,index);
      }

      public List subList(int fromIndex, int toIndex)
      {
         if( fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
         if( toIndex > size())
            throw new IndexOutOfBoundsException("toIndex = " + toIndex + " but size() =" + size());
         if( fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex ("+fromIndex+") must be less than toIndex("+toIndex+")");
         if(fromIndex == toIndex)            // request for empty list?
            return new LinkedList();
         return new MyCachedSubListImpl(this, fromIndex, toIndex);
      }

      private void checkIndex(int i) {
         if(size() == 0) return; // No need to check here.
         if( i < 0 || i >= size() ) {
            throw new IndexOutOfBoundsException("Index out of bound at CachedListImpl(). Index is " +i
            + " but size is " +size());
         }
      }

   }

}
