/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.interceptors;

import org.jboss.cache.*;
import org.jboss.cache.optimistic.TransactionWorkspace;

import javax.transaction.TransactionManager;

/**
 * Abstract interceptor for optimistic locking
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class OptimisticInterceptor extends Interceptor
{
    protected TransactionManager txManager = null;
    protected TransactionTable txTable = null;

    public void setCache(TreeCache cache)
    {
        //super.setCache(cache);
        this.cache = cache;
        txManager = cache.getTransactionManager();
        txTable = cache.getTransactionTable();
    }

    protected TransactionWorkspace getTransactionWorkspace(GlobalTransaction gtx) throws CacheException
    {
        OptimisticTransactionEntry transactionEntry = (OptimisticTransactionEntry) txTable.get(gtx);

        if (transactionEntry == null)
        {
            throw new CacheException("unable to map global transaction " + gtx + " to transaction entry");
        }

        // try and get the workspace from the transaction
        return transactionEntry.getTransactionWorkSpace();
    }
}
