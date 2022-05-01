/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.TreeCache;

import java.util.*;

/**
 * CacheLoader implementation which delegates to another TreeCache. This allows to stack caches on top of each
 * other, allowing for hierarchical cache levels. For example, first level cache delegates to a second level cache,
 * which delegates to a persistent cache.
 * @author Bela Ban
 * @author Daniel Gredler
 * @version $Id: DelegatingCacheLoader.java 1033 2006-01-17 15:31:21Z bela $
 */
public abstract class DelegatingCacheLoader implements CacheLoader {
   Log    log=LogFactory.getLog(getClass());
   /** HashMap<Object,List<Modification>>. List of open transactions. Note that this is purely transient, as
    * we don't use a log, recovery is not available */
   HashMap   transactions=new HashMap();

   public static final int delegateGetChildrenNames =  1;
   public static final int delegateGetKey           =  2;
   public static final int delegateGet              =  3;
   public static final int delegateExists           =  4;
   public static final int delegatePutKeyVal        =  5;
   public static final int delegatePut              =  6;
   public static final int delegateRemoveKey        =  7;
   public static final int delegateRemove           =  8;
   public static final int delegateRemoveData       =  9;
   public static final int delegateLoadEntireState  = 10;
   public static final int delegateStoreEntireState = 11;
   public static final int putList                  = 12;



   public abstract void setConfig(Properties props);

   public abstract void setCache(TreeCache c);

   public Set getChildrenNames(Fqn fqn) throws Exception {
      Set retval=delegateGetChildrenNames(fqn);
      return retval == null? null : (retval.size() == 0? null : retval);
   }

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

//   public Object get(Fqn name, Object key) throws Exception {
//      return delegateGet(name, key);
//   }

   public Map get(Fqn name) throws Exception
   {
       // See http://jira.jboss.com/jira/browse/JBCACHE-118
       return delegateGet(name);
   }

   public boolean exists(Fqn name) throws Exception {
      return delegateExists(name);
   }

   public Object put(Fqn name, Object key, Object value) throws Exception {
      return delegatePut(name, key, value);
   }

   public void put(Fqn name, Map attributes) throws Exception {
      delegatePut(name, attributes);
   }


   public void put(Fqn fqn, Map attributes, boolean erase) throws Exception {
      if(erase)
         removeData(fqn);
      put(fqn, attributes);
   }

   public void put(List modifications) throws Exception {
      if(modifications == null || modifications.size() == 0) return;
      delegatePut(modifications);
   }

   public Object remove(Fqn name, Object key) throws Exception {
      return delegateRemove(name, key);
   }

   public void remove(Fqn name) throws Exception {
      delegateRemove(name);
   }

   public void removeData(Fqn name) throws Exception {
      delegateRemoveData(name);
   }

   public void prepare(Object tx, List modifications, boolean one_phase) throws Exception {
      if(one_phase)
         put(modifications);
      else
         transactions.put(tx, modifications);
   }

   public void commit(Object tx) throws Exception {
      List modifications=(List)transactions.get(tx);
      if(modifications == null)
         throw new Exception("transaction " + tx + " not found in transaction table");
      put(modifications);
   }

   public void rollback(Object tx) {
      transactions.remove(tx);
   }

   public byte[] loadEntireState() throws Exception {
      return delegateLoadEntireState();
   }

   public void storeEntireState(byte[] state) throws Exception {
      delegateStoreEntireState(state);
   }

//   protected void handleModifications(Modification modifications) {
//      for(Iterator it=modifications.iterator(); it.hasNext();) {
//         Modification m=(Modification)it.next();
//         switch(m.getType()) {
//            case Modification.PUT_DATA:
//               put(m.getFqn(), m.getData());
//               break;
//            case Modification.PUT_DATA_ERASE:
//               put(m.getFqn(), m.getData(), true);
//               break;
//            case Modification.PUT_KEY_VALUE:
//               put(m.getFqn(), m.getKey(), m.getValue());
//               break;
//            case Modification.REMOVE_DATA:
//               removeData(m.getFqn());
//               break;
//            case Modification.REMOVE_KEY_VALUE:
//               remove(m.getFqn(), m.getKey());
//               break;
//            case Modification.REMOVE_NODE:
//               remove(m.getFqn());
//               break;
//            default:
//               log.error("modification type " + m.getType() + " not known");
//               break;
//         }
//      }
//
//   }

   public void create() throws Exception {
      // Empty.
   }

   public void start() throws Exception {
      // Empty.
   }

   public void stop() {
      // Empty.
   }

   public void destroy() {
      // Empty.
   }

   /*------------------------------ Delegating Methods ------------------------------*/




   protected abstract Set delegateGetChildrenNames(Fqn fqn) throws Exception;

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

//   protected abstract Object delegateGet(Fqn name, Object key) throws Exception;

   protected abstract Map delegateGet(Fqn name) throws Exception;

   protected abstract boolean delegateExists(Fqn name) throws Exception;

   protected abstract Object delegatePut(Fqn name, Object key, Object value) throws Exception;

   protected abstract void delegatePut(Fqn name, Map attributes) throws Exception;

   protected abstract Object delegateRemove(Fqn name, Object key) throws Exception;

   protected abstract void delegateRemove(Fqn name) throws Exception;

   protected abstract void delegateRemoveData(Fqn name) throws Exception;

   protected abstract byte[] delegateLoadEntireState() throws Exception;

   protected abstract void delegateStoreEntireState(byte[] state) throws Exception;

   protected abstract void delegatePut(List modifications) throws Exception;
}
