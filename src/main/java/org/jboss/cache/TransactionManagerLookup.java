package org.jboss.cache;

import javax.transaction.TransactionManager;

/**
 * Factory interface, allows TreeCache to use different transactional systems.
 *
 * @author Bela Ban, Aug 26 2003
 * @version $Id: TransactionManagerLookup.java 1523 2006-04-08 20:30:08Z genman $
 */
public interface TransactionManagerLookup {

   /**
    * Returns a new TransactionManager.
    * @throws Exception if lookup failed
    */
   TransactionManager getTransactionManager() throws Exception;

}
