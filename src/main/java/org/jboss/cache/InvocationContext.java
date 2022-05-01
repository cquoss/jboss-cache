/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;

import org.jboss.cache.config.Option;

import javax.transaction.Transaction;

/**
 * This context holds information specific to a method invocation.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class InvocationContext
{
    private Transaction transaction;
    private GlobalTransaction globalTransaction;
    private Option optionOverrides;
    // defaults to true.
    private boolean originLocal = true;
    private boolean txHasMods;
    
    public void setLocalRollbackOnly(boolean localRollbackOnly)
    {
        this.localRollbackOnly = localRollbackOnly;
    }

    private boolean localRollbackOnly;

    /**
     * Retrieves the transaction associated with this invocation
     * @return The transaction associated with this invocation
     */
    public Transaction getTransaction()
    {
        return transaction;
    }

    /**
     * Sets the transaction associated with this invocation
     * @param transaction
     */
    public void setTransaction(Transaction transaction)
    {
        this.transaction = transaction;
    }

    /**
     * Retrieves the global transaction associated with this invocation
     * @return the global transaction associated with this invocation
     */
    public GlobalTransaction getGlobalTransaction()
    {
        return globalTransaction;
    }

    /**
     * Sets the global transaction associated with this invocation
     * @param globalTransaction
     */
    public void setGlobalTransaction(GlobalTransaction globalTransaction)
    {
        this.globalTransaction = globalTransaction;
    }

    /**
     * Retrieves the option overrides associated with this invocation
     * @return the option overrides associated with this invocation
     */
    public Option getOptionOverrides()
    {
       if (optionOverrides == null) optionOverrides = new Option();
       return optionOverrides;
    }

    /**
     * Sets the option overrides associated with this invocation
     * @param optionOverrides
     */
    public void setOptionOverrides(Option optionOverrides)
    {
        this.optionOverrides = optionOverrides;
    }

    /**
     * Tests if this invocation originated locally or from a remote cache.
     * @return true if the invocation originated locally.
     */
    public boolean isOriginLocal()
    {
        return originLocal;
    }

    /**
     * If set to true, the invocation is assumed to have originated locally.  If set to false,
     * assumed to have originated from a remote cache.
     * @param originLocal
     */
    public void setOriginLocal(boolean originLocal)
    {
        this.originLocal = originLocal;
    }


    public String toString()
    {
        return "InvocationContext{" +
                "transaction=" + transaction +
                ", globalTransaction=" + globalTransaction +
                ", optionOverrides=" + optionOverrides +
                ", originLocal=" + originLocal +
                ", txHasMods=" + txHasMods +
                '}';
    }

    public boolean isTxHasMods()
    {
        return txHasMods;
    }

    public void setTxHasMods(boolean b)
    {
        txHasMods = b;
    }

    public boolean isLocalRollbackOnly()
    {
        return localRollbackOnly;
    }
}
