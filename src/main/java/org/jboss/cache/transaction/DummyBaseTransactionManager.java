package org.jboss.cache.transaction;

import javax.transaction.*;

/**
 * @author bela
 * @version $Revision: 1378 $
 *          Date: May 15, 2003
 *          Time: 4:11:37 PM
 */
public class DummyBaseTransactionManager implements TransactionManager, java.io.Serializable {
   static ThreadLocal thread_local=new ThreadLocal();
    private static final long serialVersionUID = -6716097342564237376l;

   /**
    * Starts a new transaction, and associate it with the calling thread.
    *
    * @throws javax.transaction.NotSupportedException If the calling thread is already
    *                               associated with a transaction, and nested transactions are
    *                               not supported.
    * @throws javax.transaction.SystemException       If the transaction service fails in an
    *                               unexpected way.
    */
   public void begin() throws NotSupportedException, SystemException {
      Transaction currentTx;
      if((currentTx=getTransaction()) != null)
         throw new NotSupportedException(Thread.currentThread() +
                                         " is already associated with a transaction (" + currentTx + ")");
      DummyTransaction tx=new DummyTransaction(this);
      setTransaction(tx);
   }

   /**
    * Commit the transaction associated with the calling thread.
    *
    * @throws javax.transaction.RollbackException          If the transaction was marked for rollback
    *                                    only, the transaction is rolled back and this exception is
    *                                    thrown.
    * @throws IllegalStateException      If the calling thread is not associated
    *                                    with a transaction.
    * @throws javax.transaction.SystemException            If the transaction service fails in an
    *                                    unexpected way.
    * @throws javax.transaction.HeuristicMixedException    If a heuristic decision was made and
    *                                    some some parts of the transaction have been committed while
    *                                    other parts have been rolled back.
    * @throws javax.transaction.HeuristicRollbackException If a heuristic decision to roll
    *                                    back the transaction was made.
    * @throws SecurityException          If the caller is not allowed to commit this
    *                                    transaction.
    */
   public void commit() throws RollbackException, HeuristicMixedException,
           HeuristicRollbackException, SecurityException,
           IllegalStateException, SystemException {
      int status;
      Transaction tx=getTransaction();
      if(tx == null)
         throw new IllegalStateException("thread not associated with transaction");
      status=tx.getStatus();
      if(status == Status.STATUS_MARKED_ROLLBACK)
         throw new RollbackException();
      tx.commit();

      // Disassociate tx from thread.
      setTransaction(null);
   }

   /**
    * Rolls back the transaction associated with the calling thread.
    *
    * @throws IllegalStateException If the transaction is in a state
    *                               where it cannot be rolled back. This could be because the
    *                               calling thread is not associated with a transaction, or
    *                               because it is in the
    *                               {@link javax.transaction.Status#STATUS_PREPARED prepared state}.
    * @throws SecurityException     If the caller is not allowed to roll back
    *                               this transaction.
    * @throws javax.transaction.SystemException       If the transaction service fails in an
    *                               unexpected way.
    */
   public void rollback() throws IllegalStateException, SecurityException,
           SystemException {
      Transaction tx=getTransaction();
      if(tx == null)
         throw new IllegalStateException("no transaction associated with thread");
      tx.rollback();

      // Disassociate tx from thread.
      setTransaction(null);
   }

   /**
    * Mark the transaction associated with the calling thread for rollback
    * only.
    *
    * @throws IllegalStateException If the transaction is in a state
    *                               where it cannot be rolled back. This could be because the
    *                               calling thread is not associated with a transaction, or
    *                               because it is in the
    *                               {@link javax.transaction.Status#STATUS_PREPARED prepared state}.
    * @throws javax.transaction.SystemException       If the transaction service fails in an
    *                               unexpected way.
    */
   public void setRollbackOnly() throws IllegalStateException, SystemException {
      Transaction tx=getTransaction();
      if(tx == null)
         throw new IllegalStateException("no transaction associated with calling thread");
      tx.setRollbackOnly();
   }

   /**
    * Get the status of the transaction associated with the calling thread.
    *
    * @return The status of the transaction. This is one of the
    *         {@link javax.transaction.Status} constants. If no transaction is associated
    *         with the calling thread,
    *         {@link javax.transaction.Status#STATUS_NO_TRANSACTION} is returned.
    * @throws javax.transaction.SystemException If the transaction service fails in an
    *                         unexpected way.
    */
   public int getStatus() throws SystemException {
      Transaction tx=getTransaction();
      return tx != null ? tx.getStatus() : Status.STATUS_NO_TRANSACTION;
   }

   /**
    * Get the transaction associated with the calling thread.
    *
    * @return The transaction associated with the calling thread, or
    *         <code>null</code> if the calling thread is not associated
    *         with a transaction.
    * @throws javax.transaction.SystemException If the transaction service fails in an
    *                         unexpected way.
    */
   public Transaction getTransaction() throws SystemException {
      return (Transaction)thread_local.get();
   }

   /**
    * Change the transaction timeout for transactions started by the calling
    * thread with the {@link #begin()} method.
    *
    * @param seconds The new timeout value, in seconds. If this parameter
    *                is <code>0</code>, the timeout value is reset to the default
    *                value.
    * @throws javax.transaction.SystemException If the transaction service fails in an
    *                         unexpected way.
    */
   public void setTransactionTimeout(int seconds) throws SystemException {
      throw new SystemException("not supported");
   }

   /**
    * Suspend the association the calling thread has to a transaction,
    * and return the suspended transaction.
    * When returning from this method, the calling thread is no longer
    * associated with a transaction.
    *
    * @return The transaction that the calling thread was associated with,
    *         or <code>null</code> if the calling thread was not associated
    *         with a transaction.
    * @throws javax.transaction.SystemException If the transaction service fails in an
    *                         unexpected way.
    */
   public Transaction suspend() throws SystemException {
      Transaction retval=getTransaction();
      setTransaction(null);
      return retval;
   }

   /**
    * Resume the association of the calling thread with the given
    * transaction.
    *
    * @param tx The transaction to be associated with the calling thread.
    * @throws javax.transaction.InvalidTransactionException If the argument does not represent
    *                                     a valid transaction.
    * @throws IllegalStateException       If the calling thread is already
    *                                     associated with a transaction.
    * @throws javax.transaction.SystemException             If the transaction service fails in an
    *                                     unexpected way.
    */
   public void resume(Transaction tx) throws InvalidTransactionException, IllegalStateException, SystemException {
      setTransaction(tx);
   }

   /**
    * Just used for unit tests
    * @param tx
    */
   public void setTransaction(Transaction tx) {
      thread_local.set(tx);
   }

}
