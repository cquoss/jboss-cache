/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import org.jboss.cache.optimistic.TransactionWorkspace;
import org.jboss.cache.optimistic.TransactionWorkspaceImpl;

/**
 * Subclasses the {@link TransactionEntry} class to add a {@link TransactionWorkspace}.  Used with optimistic locking
 * where each call is assigned a trasnaction and a transaction workspace.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:stevew@jofti.com">Steve Woodcock (stevew@jofti.com)</a>
 */

public class OptimisticTransactionEntry extends TransactionEntry{

   private TransactionWorkspace transactionWorkSpace = new TransactionWorkspaceImpl();

   public OptimisticTransactionEntry() {
   }

   public String toString() {
      StringBuffer sb = new StringBuffer(super.toString());
      sb.append("\nworkspace: ").append(transactionWorkSpace);
      return sb.toString();
   }

    /**
     * @return Returns the transactionWorkSpace.
     */
    public TransactionWorkspace getTransactionWorkSpace() {
        return transactionWorkSpace;
    }
    
    /**
     * @param transactionWorkSpace The transactionWorkSpace to set.
     */
    public void setTransactionWorkSpace(
            TransactionWorkspace transactionWorkSpace) {
        this.transactionWorkSpace = transactionWorkSpace;
    }

}
