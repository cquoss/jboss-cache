/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;

import java.util.Map;

/**
 * Interface for PojoCache. User should use this interface directly to access the public APIs.
 * <p>PojoCache is an in-memory, transactional, fine-grained, and object-oriented cache system. It
 * differs from the typical generic cache library by operating on the pojo level directly without requiring
 * that object to be serializable (although it does require "aspectizing" the POJO). Further, it can
 * preserve object graph during replication or persistency. It also track the replication via fine-grained
 * maner, i.e., only changed fields are replicated.</p>
 *
 * @author Ben Wang
 * @since 1.4
 */
public interface PojoCacheIfc
{
   /**
     * Retrieve pojo from the cache. Return null if object does not exist in the cache.
    * Note that this operation is fast if there is already an POJO instance attached to the cache.
     *
     * @param fqn Instance that associates with this node.
     * @return Current content value. Null if does not exist.
     * @throws CacheException Throws if there is an error related to the cache operation, e.g.,
    * {@link org.jboss.cache.lock.TimeoutException}.
     */
   Object getObject(String fqn) throws CacheException;

   /**
     * Retrieve pojo object from the cache. Return null if object does not exist in the cache.
    * Note that this operation is fast if there is already an POJO instance attached to the cache.
     *
     * @param fqn Instance that associates with this node.
     * @return Current content value. Null if does not exist.
    * @throws CacheException Throws if there is an error related to the cache operation, e.g.,
   * {@link org.jboss.cache.lock.TimeoutException}.
     */
   Object getObject(Fqn fqn) throws CacheException;

   /**
    * Insert a pojo into the cache.
    * It will also recursively put the any sub-object that is
    * declared as aop-capable (i.e., in <code>jboss-aop.xml</code>).
    * Note that <code>List</code>, <code>Map</code>, <code>Set</code>
    * attributes are aop-enabled, by default, as well.
    *
    * @param fqn The fqn instance to associate with the object in the cache.
    * @param obj aop-enabled object to be inerted into the cache. If null,
    *            it will nullify the fqn node.
    * @param obj Return the previous content under fqn.
    * @return Existing POJO or null if there is not.
    * @throws CacheException Throws if there is an error related to the cache operation, e.g.,
   * {@link org.jboss.cache.lock.TimeoutException}.
    */
   Object putObject(String fqn, Object obj) throws CacheException;

   /**
    * Insert a pojo into the cache.
    * It will also recursively put the any sub-object that is
    * declared as aop-capable (i.e., in <code>jboss-aop.xml</code>).
    * Note that <code>List</code>, <code>Map</code>, <code>Set</code>
    * attributes are aop-enabled, by default, as well.
    *
    * @param fqn The fqn instance to associate with the object in the cache.
    * @param obj aop-enabled object to be inerted into the cache. If null,
    *            it will nullify the fqn node.
    * @param obj Return the previous content under fqn.
    * @return Existing POJO or null if there is not.
    * @throws CacheException Throws if there is an error related to the cache operation, e.g.,
   * {@link org.jboss.cache.lock.TimeoutException}.
    */
   Object putObject(Fqn fqn, Object obj) throws CacheException;

   /**
    * Remove pojo object from the cache.
    *
    * @param fqn Instance that associates with this node.
    * @return Original value object from this node.
    * @throws CacheException Throws if there is an error related to the cache operation, e.g.,
   * {@link org.jboss.cache.lock.TimeoutException}.
    */
   Object removeObject(String fqn) throws CacheException;

   /**
    * Remove pojo object from the cache.
    *
    * @param fqn Instance that associates with this node.
    * @return Original value object from this node.
    * @throws CacheException Throws if there is an error related to the cache operation, e.g.,
   * {@link org.jboss.cache.lock.TimeoutException}.
    */
   Object removeObject(Fqn fqn) throws CacheException;

   /**
    * Query all managed pojo objects under the fqn recursively. Note that this will not return the sub-object pojos,
    * e.g., if Person has a sub-object of Address, it won't return Address pojo. Note also that this operation is
    * not thread-safe now. In addition, it assumes that once a pojo is found with a fqn, no more pojo is stored
    * under the children of the fqn. That is, we don't mixed the fqn with different pojos.
    * @param fqn The starting place to find all pojos.
    * @return Map of all pojos found with (fqn, pojo) pair. Return size of 0, if not found.
    * @throws CacheException Throws if there is an error related to the cache operation, e.g.,
   * {@link org.jboss.cache.lock.TimeoutException}.
    */
   Map findObjects(String fqn) throws CacheException;

   /**
    * Query all managed pojo objects under the fqn recursively. Note that this will not return the sub-object pojos,
    * e.g., if Person has a sub-object of Address, it won't return Address pojo. Note also that this operation is
    * not thread-safe now. In addition, it assumes that once a pojo is found with a fqn, no more pojo is stored
    * under the children of the fqn. That is, we don't mixed the fqn with different pojos.
    * @param fqn The starting place to find all pojos.
    * @return Map of all pojos found with (fqn, pojo) pair. Return size of 0, if not found.
    * @throws CacheException
    */
   Map findObjects(Fqn fqn) throws CacheException;

   /**
    * If the flag is set, then POJO which is not instrumented under AOP and which is not implementing the
    * Serializable interface will still be marshalled and make serializable.
    *
    */
   void setMarshallNonSerializable(boolean marshall);

   /**
    * Indicate whether the flag is set or not.
    */
   boolean isMarshallNonSerializable();

   /**
    * Obtain a cache aop type for user to traverse the defined "primitive" types in aop.
    *
    * @param clazz The original pojo class
    * @return CachedType
    */
   public CachedType getCachedType(Class clazz);

}
