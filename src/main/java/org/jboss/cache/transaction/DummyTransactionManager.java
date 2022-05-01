package org.jboss.cache.transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

/**
 * @author bela
 * @version $Revision: 1259 $
 *          Date: May 15, 2003
 *          Time: 4:11:37 PM
 */
public class DummyTransactionManager extends DummyBaseTransactionManager {
   static DummyTransactionManager instance=null;
   static Log log=LogFactory.getLog(DummyTransactionManager.class);
   private static final long serialVersionUID = 4396695354693176535L; 

   public DummyTransactionManager() {
      ;
   }

   public static DummyTransactionManager getInstance() {
      if(instance == null) {
         instance=new DummyTransactionManager();
         try {
            Properties p=new Properties();
            p.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.cache.transaction.DummyContextFactory");
            Context ctx=new InitialContext(p);
            ctx.bind("java:/TransactionManager", instance);
            ctx.bind("UserTransaction", new DummyUserTransaction(instance));
         }
         catch(NamingException e) {
            log.error("binding of DummyTransactionManager failed", e);
         }
      }
      return instance;
   }

   public static void destroy() {
      if(instance == null) return;
      try {
         Properties p=new Properties();
         p.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.cache.transaction.DummyContextFactory");
         Context ctx=new InitialContext(p);
         ctx.unbind("java:/TransactionManager");
         ctx.unbind("UserTransaction");
      }
      catch(NamingException e) {
         log.error("unbinding of DummyTransactionManager failed", e);
      }
      instance.setTransaction(null);
      instance=null;
   }

}
