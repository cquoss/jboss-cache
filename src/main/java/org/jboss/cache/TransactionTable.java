/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.lock.IdentityLock;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * Maintains the mapping between local (Transaction) and global transactions
 * (GlobalTransaction). Also keys modifications and undo-operations) under a
 * given TX.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 14, 2003
 * @version $Revision: 4509 $
 */
public class TransactionTable {

    /** Map<Transaction,GlobalTransaction>, mapping between local (javax.transaction.Transaction)
    * and GlobalTransactions. New: a local TX can have a number of GTXs */
   protected Map tx_map=new ConcurrentHashMap();

   /** Map<GlobalTransaction,TransactionEntry>, mappings between GlobalTransactions and modifications */
   protected Map txs=new ConcurrentHashMap();

   /** our logger */
   private static final Log log=LogFactory.getLog(TransactionTable.class);

   /**
    * Constructs a new table.
    */
   public TransactionTable() {
   }

   /**
    * Returns the number of local transactions.
    */
   public int getNumLocalTransactions() {
      return tx_map.size();
   }

   /**
    * Returns the number of global transactions.
    */
   public int getNumGlobalTransactions() {
      return txs.size();
   }

   /**
    * Returns the global transaction associated with the local transaction.
    * Returns null if tx is null or it was not found.
    */
   public GlobalTransaction get(Transaction tx) {
      if (tx == null) return null;
      return (GlobalTransaction) tx_map.get(tx);
   }

   /** 
    * Returns the local transaction associated with a GlobalTransaction. Not
    * very efficient as the values have to be iterated over, don't use
    * frequently
    *
    * @param gtx The GlobalTransaction
    * @return Transaction. The local transaction associated with a given
    * GlobalTransaction). This will be null if no local transaction is
    * associated with a given GTX
    */
   public Transaction getLocalTransaction(GlobalTransaction gtx) {
      Map.Entry entry;
      Transaction local_tx;
      GlobalTransaction global_tx;

      if(gtx == null)
         return null;
      for(Iterator it=tx_map.entrySet().iterator(); it.hasNext();) {
         entry=(Map.Entry)it.next();
         local_tx=(Transaction)entry.getKey();
         global_tx=(GlobalTransaction)entry.getValue();
         if(gtx.equals(global_tx)) {
            return local_tx;
         }
      }
      return null;
   }

   /**
    * Associates the global transaction with the local transaction.
    */
   public void put(Transaction tx, GlobalTransaction gtx) {
      if(tx == null) {
         log.error("key (Transaction) is null");
         return;
      }
      tx_map.put(tx, gtx);
   }

   /**
    * Returns the local transaction entry for the global transaction.
    * Returns null if tx is null or it was not found.
    */
   public TransactionEntry get(GlobalTransaction gtx) {
      return gtx != null ? (TransactionEntry)txs.get(gtx) : null;
   }

   /**
    * Associates the global transaction with a transaction entry.
    */
   public void put(GlobalTransaction tx, TransactionEntry entry) {
      if(tx == null) {
         log.error("key (GlobalTransaction) is null");
         return;
      }
      txs.put(tx, entry);
   }

   /**
    * Removes a global transation, returns the old transaction entry.
    */
   public TransactionEntry remove(GlobalTransaction tx) {
      return (TransactionEntry)txs.remove(tx);
   }

   /**
    * Removes a local transation, returns the global transaction entry.
    */
   public GlobalTransaction remove(Transaction tx) {
      if(tx == null)
         return null;
      return (GlobalTransaction)tx_map.remove(tx);
   }

   /**
    * Adds a motification to the global transaction.
    */
   public void addModification(GlobalTransaction gtx, MethodCall m) {
      TransactionEntry entry=get(gtx);
      if(entry == null) {
         log.error("transaction not found (gtx=" + gtx + ")");
         return;
      }
      entry.addModification(m);
   }

    public void addCacheLoaderModification(GlobalTransaction gtx, MethodCall m)
    {
        TransactionEntry entry = get(gtx);
        if(entry == null) {
         log.error("transaction not found (gtx=" + gtx + ")");
         return;
      }
      entry.addCacheLoaderModification(m);        
    }


   /**
    * Adds an undo operation to the global transaction.
    */
   public void addUndoOperation(GlobalTransaction gtx, MethodCall m) {
      TransactionEntry entry=get(gtx);
      if(entry == null) {
         log.error("transaction not found (gtx=" + gtx + ")");
         return;
      }
      entry.addUndoOperation(m);
   }

   /**
    * Adds a lock to the global transaction.
    */
   public void addLock(GlobalTransaction gtx, IdentityLock l) {
      TransactionEntry entry=get(gtx);
      if(entry == null) {
         log.error("transaction entry not found for (gtx=" + gtx + ")");
         return;
      }
      entry.addLock(l);
   }

   /**
    * Adds a collection of locks to the global transaction.
    */
   public void addLocks(GlobalTransaction gtx, Collection locks) {
      TransactionEntry entry=get(gtx);
      if(entry == null) {
         log.error("transaction entry not found for (gtx=" + gtx + ")");
         return;
      }
      entry.addLocks(locks);
   }

   /**
    * Adds a node that has been removed to the global transaction
    */
    public void addRemovedNode(GlobalTransaction gtx, Fqn fqn)
    {
       TransactionEntry entry=get(gtx);
       if(entry == null) {
          log.error("transaction entry not found for (gtx=" + gtx + ")");
         return;
       }
       entry.addRemovedNode(fqn);
    }

   /**
    * Returns summary debug information.
    */
   public String toString() {
      StringBuffer sb=new StringBuffer();
      sb.append(tx_map.size()).append(" mappings, ");
      sb.append(txs.size()).append(" transactions");
      return sb.toString();
   }

   /**
    * Returns detailed debug information.
    */
   public String toString(boolean print_details) {
      if(!print_details)
         return toString();
      StringBuffer sb=new StringBuffer();
      Map.Entry entry;
      sb.append("LocalTransactions: ").append(tx_map.size()).append("\n");
      sb.append("GlobalTransactions: ").append(txs.size()).append("\n");
      sb.append("tx_map:\n");
      for(Iterator it=tx_map.entrySet().iterator(); it.hasNext();) {
         entry=(Map.Entry)it.next();
         sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
      }
      sb.append("txs:\n");
      for(Iterator it=txs.entrySet().iterator(); it.hasNext();) {
         entry=(Map.Entry)it.next();
         sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
      }
      return sb.toString();
   }

}
