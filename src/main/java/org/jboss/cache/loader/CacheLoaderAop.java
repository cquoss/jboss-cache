package org.jboss.cache.loader;

import org.jboss.cache.Fqn;

/**
 * Responsible for storing and retrieving objects to/from secondary storage.
 *
 * @author Bela Ban Oct 31, 2003
 * @version $Id: CacheLoaderAop.java 2073 2006-06-19 12:33:28Z  $
 */
public interface CacheLoaderAop extends CacheLoader {

   /**
    * Loads an object from a persistent store.
    *
    * @param name The key under which the object is stored
    * @return The object
    * @throws Exception Thrown if the object cannot be loaded
    */
   Object loadObject(Fqn name) throws Exception;

   /**
    * Stores an object under a given key in the persistent store. If the object is already present, it will
    * be overwritten
    *
    * @param name
    * @param pojo
    * @throws Exception
    */
   void storeObject(Fqn name, Object pojo) throws Exception;

   /**
    * Removes the object with the given key from the persistent store.
    *
    * @param name
    * @throws Exception
    */
   void removeObject(Fqn name) throws Exception;
}
