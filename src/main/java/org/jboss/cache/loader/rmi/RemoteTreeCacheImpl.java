/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader.rmi;

import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.Node;
import org.jboss.cache.TreeCacheMBean;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of the {@link org.jboss.cache.TreeCache}'s remote interface.
 * 
 * @author Daniel Gredler
 * @version $Id: RemoteTreeCacheImpl.java 1424 2006-03-15 16:02:41Z genman $
 */
public class RemoteTreeCacheImpl extends UnicastRemoteObject implements RemoteTreeCache {

   private static final long serialVersionUID = 3096209368650710385L;
   
   private TreeCacheMBean cache;

   /**
    * @throws RemoteException
    */
   public RemoteTreeCacheImpl(TreeCacheMBean cache) throws RemoteException {
      this.cache=cache;
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#getChildrenNames(org.jboss.cache.Fqn)
    */
   public Set getChildrenNames(Fqn fqn) throws Exception, RemoteException {
      return this.cache.getChildrenNames(fqn);
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#get(org.jboss.cache.Fqn, java.lang.Object)
    */
   public Object get(Fqn name, Object key) throws Exception, RemoteException {
      return this.cache.get(name, key);
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#get(org.jboss.cache.Fqn)
    */
   public Node get(Fqn name) throws Exception, RemoteException {
      return this.cache.get(name);
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#exists(org.jboss.cache.Fqn)
    */
   public boolean exists(Fqn name) throws Exception, RemoteException {
      return this.cache.exists(name);
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#put(org.jboss.cache.Fqn, java.lang.Object, java.lang.Object)
    */
   public Object put(Fqn name, Object key, Object value) throws Exception, RemoteException {
      return this.cache.put(name, key, value);
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#put(org.jboss.cache.Fqn, java.util.Map)
    */
   public void put(Fqn name, Map attributes) throws Exception, RemoteException {
      this.cache.put(name, attributes);
   }

   public void put(List modifications) throws Exception, RemoteException {
      for(Iterator it=modifications.iterator(); it.hasNext();) {
         Modification m=(Modification)it.next();
         switch(m.getType()) {
            case Modification.PUT_DATA:
            case Modification.PUT_DATA_ERASE:
               cache.put(m.getFqn(), m.getData());
               break;
            case Modification.PUT_KEY_VALUE:
               cache.put(m.getFqn(), m.getKey(), m.getValue());
               break;
            case Modification.REMOVE_DATA:
               cache.removeData(m.getFqn());
               break;
            case Modification.REMOVE_KEY_VALUE:
               cache.remove(m.getFqn(), m.getKey());
               break;
            case Modification.REMOVE_NODE:
               cache.remove(m.getFqn());
               break;
            default:
               System.err.println("modification type " + m.getType() + " not known");
               break;
         }
      }

   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#remove(org.jboss.cache.Fqn, java.lang.Object)
    */
   public Object remove(Fqn name, Object key) throws Exception, RemoteException {
      return this.cache.remove(name, key);
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#remove(org.jboss.cache.Fqn)
    */
   public void remove(Fqn name) throws Exception, RemoteException {
      this.cache.remove(name);
   }

   /**
    * @see org.jboss.cache.loader.rmi.RemoteTreeCache#removeData(org.jboss.cache.Fqn)
    */
   public void removeData(Fqn name) throws Exception, RemoteException {
      this.cache.removeData(name);
   }
}
