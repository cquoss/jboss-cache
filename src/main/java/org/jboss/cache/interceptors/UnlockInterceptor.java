package org.jboss.cache.interceptors;

import org.jboss.cache.TreeCache;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.lock.IdentityLock;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Transaction;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * When a call returns, unlocks all locks held by the current thread in the
 * LockTable. This is a no-op if a transaction is used.
 *
 * @author Bela Ban
 * @version $Id: UnlockInterceptor.java 1848 2006-05-06 09:53:11Z msurtani $
 */
public class UnlockInterceptor extends Interceptor {

   Map lock_table = null;
   boolean trace = log.isTraceEnabled();

   public void setCache(TreeCache cache) {
      super.setCache(cache);
      lock_table = cache.getLockTable();
   }

   public Object invoke(MethodCall m) throws Throwable {
      try {
         return super.invoke(m);
      }
      finally
      {
         InvocationContext ctx = getInvocationContext();
         if (ctx.getOptionOverrides() == null || !ctx.getOptionOverrides().isSuppressLocking())
         {
             Transaction tx = ctx.getTransaction();
             if (tx != null && isValid(tx))
             {
                 // if (trace) log.trace("Do not do anything; we have a transaction running or node locking is optimistic.");
             }
             else { // no TX
                Thread currentThread = Thread.currentThread();
                List locks = (List)lock_table.get(currentThread);
                if (trace) log.trace("Attempting to release locks on current thread.  Lock table is " + lock_table);

                if (locks != null && locks.size() > 0) {
                   releaseLocks(locks, currentThread);
                   lock_table.remove(currentThread);
                }
             }
         }
      }
   }

   private void releaseLocks(List locks, Thread currentThread) {
      IdentityLock lock;
      for (ListIterator it=locks.listIterator(locks.size()); it.hasPrevious();) {
            lock=(IdentityLock)it.previous();
         if (trace)
               log.trace("releasing lock for " + lock.getFqn() + ": " + lock);
         lock.release(currentThread);
      }
   }


}
