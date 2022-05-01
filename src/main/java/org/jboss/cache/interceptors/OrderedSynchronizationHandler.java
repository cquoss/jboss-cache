package org.jboss.cache.interceptors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Maintains a list of Synchronization handlers. Reason is that we have to
 * invoke certain handlers <em>before</em> others. See the description in
 * SyncTxUnitTestCase.testConcurrentPuts(). For example, for synchronous
 * replication, we have to execute the ReplicationInterceptor's
 * afterCompletion() <em>before</em> the TransactionInterceptor's.
 *
 * @author Bela Ban
 * @version $Id: OrderedSynchronizationHandler.java 4310 2007-08-23 16:46:05Z manik.surtani@jboss.com $
 */
public class OrderedSynchronizationHandler implements Synchronization {
   Transaction       tx=null;
   LinkedList        handlers=new LinkedList();

   /** Map<Transaction,OrderedSynchronizationHandler> */
   static Map instances=new HashMap();

   static Log log=LogFactory.getLog(OrderedSynchronizationHandler.class);


   private OrderedSynchronizationHandler(Transaction tx) {
      this.tx=tx;
   }

   /**
    * Creates a new instance of OrderedSynchronizationHandler, or fetches an existing instance. Key is the local
    * transaction (tx). This instance registers with the TransactionManager automatically
    * @param tx
    * @return
    */
   public static OrderedSynchronizationHandler getInstance(Transaction tx) throws SystemException, RollbackException {
      OrderedSynchronizationHandler retval=(OrderedSynchronizationHandler)instances.get(tx);
      if(retval != null) return retval;
      retval=new OrderedSynchronizationHandler(tx);
      tx.registerSynchronization(retval);
      instances.put(tx, retval);
      return retval;
   }


   public void registerAtHead(Synchronization handler) {
      register(handler, true);
   }

   public void registerAtTail(Synchronization handler) {
      register(handler,  false);
   }

   void register(Synchronization handler, boolean head) {
      if(handler != null && !handlers.contains(handler)) {
         if(head)
            handlers.addFirst(handler);
         else
            handlers.addLast(handler);
      }
   }

   public void beforeCompletion() {
      for(Iterator it=handlers.iterator(); it.hasNext();) {
         Synchronization sync=(Synchronization)it.next();
         sync.beforeCompletion();
      }
   }

   public void afterCompletion(int status) {
      RuntimeException exceptionInAfterCompletion = null;
      for(Iterator it=handlers.iterator(); it.hasNext();) {
         Synchronization sync=(Synchronization)it.next();
         try {
            sync.afterCompletion(status);
         }
         catch(Throwable t) {
            log.error("failed calling afterCompletion() on " + sync, t);
            exceptionInAfterCompletion = (RuntimeException) t;
         }
      }

      // finally unregister us from the hashmap
      instances.remove(tx);

      // throw the exception so the TM can deal with it.
      if (exceptionInAfterCompletion != null) throw exceptionInAfterCompletion;
   }

   public String toString() {
      StringBuffer sb=new StringBuffer();
      sb.append("tx=" + getTxAsString() + ", handlers=" + handlers);
      return sb.toString();
   }
   
   private String getTxAsString()
   {
      // JBCACHE-1114 -- don't call toString() on tx or it can lead to stack overflow
      if (tx == null)
         return null;
      
      return tx.getClass().getName() + "@" + System.identityHashCode(tx);
   }
}
