package org.jboss.cache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.transaction.DummyTransactionManager;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;
import java.lang.reflect.Method;

/**
 * A generic class that chooses the best-fit TransactionManager. Tries a number of well-known appservers
 *
 * @author Markus Plesser
 * @version $Id: GenericTransactionManagerLookup.java 203 2005-07-08 11:09:20Z bela $
 */
public class GenericTransactionManagerLookup implements TransactionManagerLookup {

   /**
    * our logger
    */
   private static Log log=LogFactory.getLog(GenericTransactionManagerLookup.class);

   /**
    * lookups performed?
    */
   private static boolean lookupDone=false;

   /**
    * no lookup available?
    */
   private static boolean lookupFailed=false;

   /**
    * the (final) used TransactionManager
    */
   private static TransactionManager tm=null;

   /**
    * JNDI locations for TransactionManagers we know of
    */
   private static String[][] knownJNDIManagers={
      {"java:/TransactionManager", "JBoss, JRun4"},
      {"java:comp/UserTransaction", "Resin, Orion, JOnAS (JOTM)"},
      {"javax.transaction.TransactionManager", "BEA WebLogic"}
   };

   /**
    * WebSphere 5.1 TransactionManagerFactory
    */
   private static final String WS_FACTORY_CLASS_5_1="com.ibm.ws.Transaction.TransactionManagerFactory";

   /**
    * WebSphere 5.0 TransactionManagerFactory
    */
   private static final String WS_FACTORY_CLASS_5_0="com.ibm.ejs.jts.jta.TransactionManagerFactory";

   /**
    * WebSphere 4.0 TransactionManagerFactory
    */
   private static final String WS_FACTORY_CLASS_4="com.ibm.ejs.jts.jta.JTSXA";

   /**
    * Get the systemwide used TransactionManager
    *
    * @return TransactionManager
    */
   public TransactionManager getTransactionManager() {
      if(!lookupDone)
         doLookups();
      if(tm != null)
         return tm;
      if(lookupFailed) {
         //fall back to a dummy from JBossCache
         tm=DummyTransactionManager.getInstance();
         log.warn("Falling back to DummyTransactionManager from JBossCache");
      }
      return tm;
   }


   /**
    * Try to figure out which TransactionManager to use
    */
   private static void doLookups() {
      if(lookupFailed)
         return;
      InitialContext ctx;
      try {
         ctx=new InitialContext();
      }
      catch(NamingException e) {
         log.error("Could not create an initial JNDI context!", e);
         lookupFailed=true;
         return;
      }
      //probe jndi lookups first
      Object jndiObject=null;
      for(int i=0; i < knownJNDIManagers.length; i++) {
         try {
            if(log.isDebugEnabled()) log.debug("Trying to lookup TransactionManager for " + knownJNDIManagers[i][1]);
            jndiObject=ctx.lookup(knownJNDIManagers[i][0]);
         }
         catch(NamingException e) {
            log.info("Failed to perform a lookup for [" + knownJNDIManagers[i][0] + " (" + knownJNDIManagers[i][1] + ")]");
         }
         if(jndiObject instanceof TransactionManager) {
            tm=(TransactionManager)jndiObject;
            log.info("Found TransactionManager for " + knownJNDIManagers[i][1]);
            return;
         }
      }
      //try to find websphere lookups since we came here
      Class clazz;
      try {
         log.debug("Trying WebSphere 5.1: " + WS_FACTORY_CLASS_5_1);
         clazz=Class.forName(WS_FACTORY_CLASS_5_1);
         log.info("Found WebSphere 5.1: " + WS_FACTORY_CLASS_5_1);
      }
      catch(ClassNotFoundException ex) {
         try {
            log.debug("Trying WebSphere 5.0: " + WS_FACTORY_CLASS_5_0);
            clazz=Class.forName(WS_FACTORY_CLASS_5_0);
            log.info("Found WebSphere 5.0: " + WS_FACTORY_CLASS_5_0);
         }
         catch(ClassNotFoundException ex2) {
            try {
               log.debug("Trying WebSphere 4: " + WS_FACTORY_CLASS_4);
               clazz=Class.forName(WS_FACTORY_CLASS_4);
               log.info("Found WebSphere 4: " + WS_FACTORY_CLASS_4);
            }
            catch(ClassNotFoundException ex3) {
               log.info("Couldn't find any WebSphere TransactionManager factory class, neither for WebSphere version 5.1 nor 5.0 nor 4");
               lookupFailed=true;
               return;
            }
         }
      }
      try {
         Class[] signature=null;
         Object[] args=null;
         Method method=clazz.getMethod("getTransactionManager", signature);
         tm=(TransactionManager)method.invoke(null, args);
      }
      catch(Exception ex) {
         log.error("Found WebSphere TransactionManager factory class [" + clazz.getName() +
                   "], but couldn't invoke its static 'getTransactionManager' method", ex);
      }
   }

}
