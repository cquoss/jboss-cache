package org.jboss.cache;

import org.jboss.cache.transaction.BatchModeTransactionManager;

import javax.transaction.TransactionManager;


/**
 * Returns an instance of DummyTransactionManager, used by standalone cache.
 *
 * @author Bela Ban Sept 5 2003
 * @version $Id: BatchModeTransactionManagerLookup.java 2073 2006-06-19 12:33:28Z  $
 */
public class BatchModeTransactionManagerLookup implements TransactionManagerLookup {

   public TransactionManager getTransactionManager() throws Exception {
      return BatchModeTransactionManager.getInstance();
   }
}
