/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader.rmi;

import org.jboss.cache.Fqn;
import org.jboss.cache.Node;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;
import java.util.List;

/**
 * Remote interface to a {@link org.jboss.cache.TreeCache} instance. Used by
 * {@link org.jboss.cache.loader.RmiDelegatingCacheLoader}.
 * 
 * @author Daniel Gredler
 * @version $Id: RemoteTreeCache.java 1282 2006-02-21 22:54:35Z bstansberry $
 */
public interface RemoteTreeCache extends Remote {
   public Set getChildrenNames(Fqn fqn) throws Exception, RemoteException;
   public Object get(Fqn name, Object key) throws Exception, RemoteException;
   public Node get(Fqn name) throws Exception, RemoteException;
   public boolean exists(Fqn name) throws Exception, RemoteException;
   public Object put(Fqn name, Object key, Object value) throws Exception, RemoteException;
   public void put(Fqn name, Map attributes) throws Exception, RemoteException;
   public void put(List modifications) throws Exception, RemoteException;
   public Object remove(Fqn name, Object key) throws Exception, RemoteException;
   public void remove(Fqn name) throws Exception, RemoteException;
   public void removeData(Fqn name) throws Exception, RemoteException;
}
