/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader;

import org.jboss.cache.Fqn;
import org.jboss.cache.DataNode;
import org.jboss.cache.TreeCache;
import org.jboss.cache.Modification;

import java.util.*;

/**
 * DelegatingCacheLoader implementation which delegates to a local (in the same VM) TreeCache. Sample code:
 * <pre>
 * TreeCache firstLevel=new TreeCache();
 * TreeCache secondLevel=new TreeCache();
 * DelegatingCacheLoader l=new DelegatingCacheLoader(secondLevel);
 * l.setCache(firstLevel);
 * firstLevel.setCacheLoader(l);
 * secondLevel.start();
 * firstLevel.start();
 * </pre>
 * @author Bela Ban
 * @author Daniel Gredler
 * @version $Id: LocalDelegatingCacheLoader.java 1031 2006-01-17 13:35:36Z bela $
 */
public class LocalDelegatingCacheLoader extends DelegatingCacheLoader {

   TreeCache delegate=null;

   public LocalDelegatingCacheLoader()
   {
   }

   public LocalDelegatingCacheLoader(TreeCache delegate) {
      this.delegate=delegate;
   }

   public void setConfig(Properties props) {
   }

   public void setCache(TreeCache cache) {
       // empty
   }

   protected Set delegateGetChildrenNames(Fqn fqn) throws Exception {
      return delegate.getChildrenNames(fqn);
   }

// See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.
//   protected Object delegateGet(Fqn name, Object key) throws Exception {
//      return delegate.get(name, key);
//   }

   protected Map delegateGet(Fqn name) throws Exception {
      DataNode n=delegate.get(name);
      if(n == null) return null;
      // after this stage we know that the node exists.  So never return a null - at worst, an empty map.
      Map m=n.getData();
      if (m == null) m = new HashMap(0);
      return m;
   }

   protected void setDelegateCache(TreeCache delegate)
   {
       this.delegate = delegate;
   }

   protected boolean delegateExists(Fqn name) throws Exception {
      return delegate.exists(name);
   }

   protected Object delegatePut(Fqn name, Object key, Object value) throws Exception {
      return delegate.put(name, key, value);
   }

   protected void delegatePut(Fqn name, Map attributes) throws Exception {
      delegate.put(name, attributes);
   }

   protected void delegatePut(List modifications) throws Exception {
      for(Iterator it=modifications.iterator(); it.hasNext();) {
         Modification m=(Modification)it.next();
         switch(m.getType()) {
            case Modification.PUT_DATA:
               put(m.getFqn(), m.getData());
               break;
            case Modification.PUT_DATA_ERASE:
               put(m.getFqn(), m.getData(), true);
               break;
            case Modification.PUT_KEY_VALUE:
               put(m.getFqn(), m.getKey(), m.getValue());
               break;
            case Modification.REMOVE_DATA:
               removeData(m.getFqn());
               break;
            case Modification.REMOVE_KEY_VALUE:
               remove(m.getFqn(), m.getKey());
               break;
            case Modification.REMOVE_NODE:
               remove(m.getFqn());
               break;
            default:
               log.error("modification type " + m.getType() + " not known");
               break;
         }
      }
   }

   protected Object delegateRemove(Fqn name, Object key) throws Exception {
      return delegate.remove(name, key);
   }

   protected void delegateRemove(Fqn name) throws Exception {
      delegate.remove(name);
   }

   protected void delegateRemoveData(Fqn name) throws Exception {
      delegate.removeData(name);
   }

   protected byte[] delegateLoadEntireState() throws Exception {
      return delegate.getStateBytes();
   }

   protected void delegateStoreEntireState(byte[] state) throws Exception {
      delegate.setStateBytes(state);
   }

}
