package org.jboss.cache;

import org.jboss.cache.transaction.DummyTransactionManager;

import javax.transaction.TransactionManager;


/**
 * Returns an instance of DummyTransactionManager, used by standalone cache.
 *
 * @author Bela Ban Sept 5 2003
 * @version $Id: DummyTransactionManagerLookup.java 2073 2006-06-19 12:33:28Z  $
 */
public class DummyTransactionManagerLookup implements TransactionManagerLookup {

   public TransactionManager getTransactionManager() throws Exception {
      return DummyTransactionManager.getInstance();
   }
}
