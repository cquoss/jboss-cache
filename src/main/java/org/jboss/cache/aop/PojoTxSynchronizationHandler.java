/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.aop.InstanceAdvisor;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import java.util.List;
import java.lang.reflect.Field;

/**
 * Handling the rollback operation for PojoCache level, specifically interceptor add/remove, etc.
 *
 * @author Ben Wang
 * @version $Id: PojoTxSynchronizationHandler.java 4071 2007-06-25 21:10:41Z jgreene $
 */

public class PojoTxSynchronizationHandler implements Synchronization {
   static Log log = LogFactory.getLog(PojoTxSynchronizationHandler.class.getName());
   private Transaction tx_;
   private PojoCache cache_;

   PojoTxSynchronizationHandler(Transaction tx, PojoCache cache)
   {
      tx_ = tx;
      cache_ = cache;
   }

   public void beforeCompletion() {
      // Not interested
   }

   public void afterCompletion(int status) {
      try {
         switch (status) {
            case Status.STATUS_COMMITTED:
               break;
            case Status.STATUS_MARKED_ROLLBACK:
            case Status.STATUS_ROLLEDBACK:
               log.debug("Running rollback phase");
               runRollbackPhase();
               log.debug("Finished rollback phase");
               break;

            default:
               throw new IllegalStateException("illegal status: " + status);
         }
      }
      finally {
         cache_.resetUndoOp();
      }
   }

   private void runRollbackPhase()
   {
      // Rollback the pojo interceptor add/remove
      List list = cache_.getModList();
      if (list != null) {
         for (int i = (list.size() - 1); i >= 0; i--) {
            ModificationEntry ent = (ModificationEntry) list.get(i);
            InstanceAdvisor advisor = ent.getInstanceAdvisor();
            BaseInterceptor interceptor = ent.getCacheInterceptor();
            switch (ent.getOpType()) {
               case ModificationEntry.INTERCEPTOR_ADD:
                  advisor.removeInterceptor(interceptor.getName());
                  break;
               case ModificationEntry.INTERCEPTOR_REMOVE:
                  advisor.appendInterceptor(interceptor);
                  break;
               case ModificationEntry.COLLECTION_REPLACE:
                  Field field = ent.getField();
                  Object key = ent.getKey();
                  Object value = ent.getOldValue();
                  try {
                     field.set(key, value);
                  } catch (IllegalAccessException e) {
                     throw new RuntimeException("PojoTxSynchronizationHandler.runRollbackPhase(): Exception: " + e);
                  }
                  break;
               default:
                  throw new IllegalArgumentException("PojoTxSynchronizationHandler.runRollbackPhase: getOptType: "
                          + ent.getOpType());
            }
         }
      }
   }
}

