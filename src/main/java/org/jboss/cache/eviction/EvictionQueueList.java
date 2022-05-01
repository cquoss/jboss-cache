/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.eviction;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Arrays;

/**
 * @author Daniel Huang (dhuang@jboss.org)
 * @version $Revision: 1747 $
 */
public class EvictionQueueList
{
   EvictionListEntry head;
   EvictionListEntry tail;
   int modCount;
   private int size;

   EvictionQueueList()
   {
      head = null;
      tail = null;
      size = 0;
      modCount = 0;
   }

   void addToTop(EvictionListEntry entry)
   {
      EvictionListEntry formerHead = head;
      head = entry;
      // if there was no previous head then this list was empty.
      if (formerHead != null)
      {
         formerHead.previous = head;
         head.next = formerHead;
         head.previous = null;
      }
      else
      {
         tail = entry;
      }
      size++;
      modCount++;
   }

   void addToBottom(EvictionListEntry entry)
   {
      EvictionListEntry formerTail = tail;
      tail = entry;
      // if there was no previous head then this list was empty.
      if (formerTail != null)
      {
         tail.previous = formerTail;
         formerTail.next = tail;
         tail.next = null;
      }
      else
      {
         head = entry;
      }
      size++;
      modCount++;
   }

   void remove(EvictionListEntry entry)
   {
      if (this.isEmpty())
      {
         return;
      }

      if (isSingleNode(entry))
      {
         head = null;
         tail = null;
      }
      else if (isTail(entry))
      {
         tail = entry.previous;
         // unlink the last node.
         entry.previous.next = null;
      }
      else if (isHead(entry))
      {
         head = entry.next;
         head.previous = null;
      }
      else
      {
         // node is in between two other nodes.
         entry.next.previous = entry.previous;
         entry.previous.next = entry.next;
      }
      size--;
      modCount++;
   }

   int size()
   {
      return this.size;
   }

   void clear()
   {
      head = null;
      tail = null;
      size = 0;
      modCount++;
   }

   EvictionListEntry getFirst()
   {
      if (head == null)
      {
         throw new NoSuchElementException("List is empty");
      }
      return head;
   }

   EvictionListEntry getLast()
   {
      if (tail == null)
      {
         throw new NoSuchElementException("List is empty");
      }
      return tail;
   }

   Iterator iterator()
   {
      return new EvictionListIterator();
   }

   NodeEntry[] toNodeEntryArray()
   {
      if (isEmpty())
      {
         return null;
      }
      NodeEntry[] ret = new NodeEntry[size];
      int i = 0;
      EvictionListEntry temp = head;

      do
      {
         ret[i] = temp.node;
         temp = temp.next;
         i++;
      }
      while (temp != null);

      return ret;
   }

   EvictionListEntry[] toArray()
   {
      if (isEmpty())
      {
         return null;
      }
      EvictionListEntry[] ret = new EvictionListEntry[size];
      int i = 0;
      EvictionListEntry temp = head;

      do
      {
         ret[i] = temp;
         temp = temp.next;
         i++;
      }
      while (temp != null);

      return ret;
   }

   void fromArray(EvictionListEntry[] array)
   {

      for (int i = 0; i < array.length; i++)
      {
         this.addToBottom(array[i]);
      }
   }

   private boolean isEmpty()
   {
      return head == null && tail == null;
   }

   private boolean isSingleNode(EvictionListEntry entry)
   {
      return isTail(entry) && isHead(entry);
   }

   private boolean isTail(EvictionListEntry entry)
   {
      return entry.next == null;
   }

   private boolean isHead(EvictionListEntry entry)
   {
      return entry.previous == null;
   }

   public String toString()
   {
      return Arrays.asList(toArray()).toString();
   }

   static class EvictionListComparator implements Comparator
   {
      Comparator nodeEntryComparator;

      EvictionListComparator(Comparator nodeEntryComparator)
      {
         this.nodeEntryComparator = nodeEntryComparator;
      }

      public int compare(Object o1, Object o2)
      {
         EvictionListEntry e1 = (EvictionListEntry) o1;
         EvictionListEntry e2 = (EvictionListEntry) o2;

         return nodeEntryComparator.compare(e1.node, e2.node);
      }
   }

   class EvictionListIterator implements ListIterator
   {
      EvictionListEntry next = head;
      EvictionListEntry previous;
      EvictionListEntry cursor;

      int initialModCount = EvictionQueueList.this.modCount;

      public boolean hasNext()
      {
         this.doConcurrentModCheck();
         return next != null;
      }

      public Object next()
      {
         this.doConcurrentModCheck();
         this.forwardCursor();
         return cursor.node;
      }

      public boolean hasPrevious()
      {
         this.doConcurrentModCheck();
         return previous != null;
      }

      public Object previous()
      {
         this.doConcurrentModCheck();
         this.rewindCursor();
         return cursor.node;
      }

      public int nextIndex()
      {
         throw new UnsupportedOperationException();
      }

      public int previousIndex()
      {
         throw new UnsupportedOperationException();
      }

      public void remove()
      {
         this.doConcurrentModCheck();
         if (cursor == null)
         {
            throw new IllegalStateException("Cannot remove from iterator when there is nothing at the current iteration point");
         }
         EvictionQueueList.this.remove(cursor);
         cursor = null;
         initialModCount++;
      }

      public void set(Object o)
      {
         this.doConcurrentModCheck();
         NodeEntry e = (NodeEntry) o;
         cursor.node = e;
      }

      public void add(Object o)
      {
         this.doConcurrentModCheck();
         // todo
//         EvictionQueueList.this.addToBottom((EvictionListEntry)o);
         initialModCount++;
      }

      private void doConcurrentModCheck()
      {
         if (EvictionQueueList.this.modCount != initialModCount)
         {
            throw new ConcurrentModificationException();
         }
      }

      private void forwardCursor()
      {
         if (next == null)
         {
            throw new NoSuchElementException("No more objects to iterate.");
         }
         previous = cursor;
         cursor = next;
         next = cursor.next;
      }

      private void rewindCursor()
      {
         if (previous == null)
         {
            throw new NoSuchElementException();
         }
         next = cursor;
         cursor = previous;
         previous = cursor.previous;
      }
   }

}

class EvictionListEntry
{
   EvictionListEntry next;
   EvictionListEntry previous;

   NodeEntry node;

   EvictionListEntry()
   {
   }

   EvictionListEntry(NodeEntry node)
   {
      this.node = node;
   }

   public boolean equals(Object o)
   {
      EvictionListEntry entry = (EvictionListEntry) o;
      return this.node.getFqn().equals(entry.node.getFqn());
   }

   public int hashCode()
   {
      return this.node.getFqn().hashCode();
   }

   public String toString()
   {
      return "EntryLE=" + node;
   }

}
