/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.config.Option;
import org.jboss.cache.lock.IdentityLock;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Transaction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * This is the value (key being the {@link GlobalTransaction}) in the transaction table
 * of TreeCache.
 * <br>A TransactionEntry maintains
 * <ul>
 * <li>Handle to local Transactions: there can be more than 1 local TX associated with a GlobalTransaction
 * <li>List of modifications (Modification)
 * <li>List of nodes that were created as part of lock acquisition. These nodes can be
 * safely deleted when a transaction is rolled back
 * <li>List of locks ({@link IdentityLock}) that have been acquired by
 * this transaction so far
 * </ul>
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 14, 2003
 * @version $Revision: 3116 $
 */
public class TransactionEntry {

   static Log log = LogFactory.getLog(TransactionEntry.class);

   /**
    * Local transaction
    */
   protected Transaction ltx=null;
   protected Option option;

   /**
    * List<MethodCall> of modifications ({@link MethodCall}). They will be replicated on TX commit
    */
   protected List modification_list=new LinkedList();
   protected List cl_mod_list = new LinkedList();

   /**
    * List<MethodCall>. List of compensating {@link org.jgroups.blocks.MethodCall} objects
    * which revert the ones in <tt>modification_list</tt>. For each entry in the modification list,
    * we have a corresponding entry in this list. A rollback will simply iterate over this list in
    * reverse to undo the modifications. Note that these undo-ops will never be replicated.
    */
   protected List undo_list=new LinkedList();

   /**
    * LinkedHashSet<IdentityLock> of locks acquired by the transaction We use
    * a LinkedHashSet because we need efficient Set semantics (same lock can
    * be added multiple times) but also need guaranteed ordering for use
    * by lock release code (see JBCCACHE-874).
    */
   private LinkedHashSet locks = new LinkedHashSet();

   /**
    * A list of dummy uninitialised nodes created by the cache loader interceptor to load data for a
    * given node in this tx.
    */
   protected List dummyNodesCreatedByCacheLoader;

   /**
    * List<Fqn> of nodes that have been removed by the transaction
    */
    protected List removedNodes = new LinkedList();

   /**
    * Constructs a new TransactionEntry.
    */
   public TransactionEntry() {
   }

   /**
    * Adds a modification to the modification list.
    */
   public void addModification(MethodCall m) {
      if (m == null) return;
      modification_list.add(m);
   }

    public void addCacheLoaderModification(MethodCall m)
    {
        if (m!=null) cl_mod_list.add(m);
    }


   /**
    * Returns all modifications.
    */
   public List getModifications() {
      return modification_list;
   }

   public List getCacheLoaderModifications()
   {
       return cl_mod_list;
   }

   /**
    * Adds an undo operation to the undo list.
    * @see #undoOperations
    */
   public void addUndoOperation(MethodCall m) {
      undo_list.add(m);
   }

   /**
    * Adds the node that has been removed.
    *
    * @param fqn
    */
    public void addRemovedNode(Fqn fqn) {
       removedNodes.add(fqn);
    }

    /**
     * Gets the list of removed nodes.
     */
    public List getRemovedNodes()
    {
       return new ArrayList(removedNodes);
    }


   /**
    * Returns the undo operations in use.
    * Note:  This list may be concurrently modified.
    */
   public List getUndoOperations() {
      return undo_list;
   }

   /**
    * Sets the local transaction for this entry.
    */
   public void setTransaction(Transaction tx) {
      ltx=tx;
   }

   /**
    * Returns a local transaction associated with this TransactionEntry
    */
   public Transaction getTransaction() {
      return ltx;
   }

   /**
    * Adds a lock to the end of the lock list.
    */
   public void addLock(IdentityLock l) {
      if(l != null) {
         synchronized(locks) {
             locks.add(l);
         }
      }
   }

   /**
    * Add multiple locks to the lock list.
    * @param newLocks Collection<IdentityLock>
    */
   public void addLocks(Collection newLocks) {
      if(newLocks != null) {
         synchronized(locks) {
            locks.addAll(newLocks);
         }
      }
   }

   /**
    * Returns the locks in use.
    * 
    * @return a defensive copy of the internal data structure.
    */
   public List getLocks() {
      synchronized(locks)
      {
         return new ArrayList(locks);
      }
   }

   /**
    * Calls {@link #releaseAllLocksFIFO}.
    * @deprecated don't think this is used anymore
    */
   public void releaseAllLocks(Object owner) {
      releaseAllLocksFIFO(owner);
      synchronized (locks) {
         locks.clear();
      }
   }

   /**
    * Releases all locks held by the owner, in reverse order of creation.
    * Clears the list of locks held.
    */
   public void releaseAllLocksLIFO(Object owner) {
      
      synchronized (locks) {
         // Copying out to an array is faster than creating an ArrayList and iterating,
         // since list creation will just copy out to an array internally
         IdentityLock[] lockArray = (IdentityLock[]) locks.toArray(new IdentityLock[locks.size()]);
         for (int i = lockArray.length -1; i >= 0; i--) {
             if (log.isTraceEnabled())
             {
                 log.trace("releasing lock for " + lockArray[i].getFqn() + " (" + lockArray[i] + ")");
             }
             lockArray[i].release(owner);
         }
         locks.clear();
      }
   }

   /**
    * Releases all locks held by the owner, in order of creation.
    * Does not clear the list of locks held.
    */
   public void releaseAllLocksFIFO(Object owner) {
      // I guess a copy would work as well
      // This seems fairly safe though
      synchronized (locks) {
         for (Iterator i = locks.iterator(); i.hasNext();) {
            IdentityLock lock = (IdentityLock)i.next();
            lock.release(owner);
             if (log.isTraceEnabled())
             {
                 log.trace("releasing lock for " + lock.getFqn() + " (" + lock + ")");
             }
         }
      }
   }

   /**
    * Posts all undo operations to the TreeCache.
    */
   public void undoOperations(TreeCache cache) {
      ArrayList l;
      synchronized (undo_list) {
        l = new ArrayList(undo_list);
      }
      for (ListIterator i = l.listIterator(l.size()); i.hasPrevious();) {
         MethodCall undo_op = (MethodCall)i.previous();
         undo(undo_op, cache);
      }
   }

   private void undo(MethodCall undo_op, TreeCache cache) {
      try {
         Object retval = undo_op.invoke(cache);
          if (retval instanceof Throwable)
          {
              throw (Throwable) retval;
          }
      } catch (Throwable t) {
         log.error("undo operation failed, error=" + t);
         log.trace(t, t);
      }
   }

   /**
    * Returns debug information about this transaction.
    */
   public String toString() {
      StringBuffer sb=new StringBuffer();
      sb.append("\nmodification_list: ").append(modification_list);
      synchronized (undo_list) {
         sb.append("\nundo_list: ").append(undo_list);
      }
      synchronized (locks) {
         sb.append("\nlocks: ").append(locks);
      }
      return sb.toString();
   }

    public void loadUninitialisedNode(Fqn fqn)
    {
        if (dummyNodesCreatedByCacheLoader == null) dummyNodesCreatedByCacheLoader = new LinkedList();
        dummyNodesCreatedByCacheLoader.add(fqn);
    }

    public List getDummyNodesCreatedByCacheLoader()
    {
        return dummyNodesCreatedByCacheLoader;
    }

    /**
     * Sets a transaction-scope option override
     * @param o
     */
    public void setOption(Option o)
    {
        this.option = o;
    }

    /**
     * Retrieves a transaction scope option override
     */
    public Option getOption()
    {
        return this.option;
    }

}
