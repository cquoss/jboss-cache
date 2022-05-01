package org.jboss.cache;

import javax.naming.InitialContext;
import javax.transaction.TransactionManager;

/**
 * Default implementation. Uses JNDI to lookup TransactionManager.
 *
 * @author Bela Ban, Aug 26 2003
 * @version $Id: JBossTransactionManagerLookup.java 2073 2006-06-19 12:33:28Z  $
 */
public class JBossTransactionManagerLookup implements TransactionManagerLookup {

   public JBossTransactionManagerLookup() {
   }

   public TransactionManager getTransactionManager() throws Exception {
      return (TransactionManager)new InitialContext().lookup("java:/TransactionManager");
   }

}
