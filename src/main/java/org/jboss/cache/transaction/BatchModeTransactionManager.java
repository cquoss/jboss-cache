package org.jboss.cache.transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Not really a transaction manager in the truest sense of the word.  Only used to batch up operations.  Proper
 * transactional symantics of rollbacks and recovery are NOT used here.  This is used by PojoCache.
 * 
 * @author bela
 * @version $Revision: 1613 $
 *          Date: May 15, 2003
 *          Time: 4:11:37 PM
 */
public class BatchModeTransactionManager extends DummyBaseTransactionManager {
   static BatchModeTransactionManager instance=null;
   static Log log=LogFactory.getLog(BatchModeTransactionManager.class);
   private static final long serialVersionUID = 5656602677430350961L;  

   public BatchModeTransactionManager() {
      ;
   }

   public static BatchModeTransactionManager getInstance() {
      if(instance == null) {
         instance=new BatchModeTransactionManager();
      }
      return instance;
   }

   public static void destroy() {
      if(instance == null) return;
      instance.setTransaction(null);
      instance=null;
   }

}
