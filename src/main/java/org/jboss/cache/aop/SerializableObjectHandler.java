/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.CacheException;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

/**
 * Handle Serializable object cache management.
 *
 * @author Ben Wang
 *         Date: Aug 4, 2005
 * @version $Id: SerializableObjectHandler.java 1957 2006-05-23 23:56:33Z bwang $
 */
public class SerializableObjectHandler  {
   protected PojoCache cache_;
   protected InternalDelegate internal_;
   protected final static Log log=LogFactory.getLog(SerializableObjectHandler.class);

   public SerializableObjectHandler(PojoCache cache, InternalDelegate internal)
   {
      cache_ = cache;
      internal_ = internal;
   }


   Object serializableObjectGet(Fqn fqn)
      throws CacheException
   {
      Object obj = internal_.get(fqn, InternalDelegate.SERIALIZED);
/*
      if(cache_.isMarshallNonSerializable() && obj instanceof MarshalledObject )
      {
         try {
            obj = ((MarshalledObject)obj).get();
            if (log.isDebugEnabled()) {
               log.debug("getObject(): since obj is non-serilaizable, we need to unmarshall it first: "
               + obj.getClass());
            }
            internal_.localPut(fqn, InternalDelegate.SERIALIZED, obj);
         } catch (Exception e) {
            e.printStackTrace();
            throw new CacheException("Exception occurred during unmarshalling of non-serializable pojo: " +
            obj + " using JBoss Serialization", e);
         }
      }
      */
      return obj;
   }


   protected boolean serializableObjectPut(Fqn fqn, Object obj)
      throws CacheException
   {
      // Note that JBoss Serialization can serialize any type now.
      if (obj instanceof Serializable || cache_.isMarshallNonSerializable()) {
         if (log.isDebugEnabled()) {
            log.debug("putObject(): obj (" + obj.getClass() + ") is non-advisable but is Serializable. ");
         }

         putIntoCache(fqn, obj);
         return true;
      }

      // If the flag is set, we will marshall it using JBossSerialization
/*      if(cache_.isMarshallNonSerializable())
      {
         if (log.isDebugEnabled()) {
            log.debug("serialiableObjectPut(): obj (" + obj.getClass() + ") is non-Serializable." +
            " Will marshall it first");
         }
         putIntoCache(fqn, obj);
         return true;
      }
*/
      return false;
   }

   protected void putIntoCache(Fqn fqn, Object obj)
           throws CacheException{
      Map map = new HashMap();
      internal_.putAopClazz(fqn, obj.getClass(), map);

      // Special optimization here.
      AOPInstance aopInstance = new AOPInstance();
      aopInstance.set(obj);
      map.put(AOPInstance.KEY, aopInstance);

      /*
      if(cache_.isMarshallNonSerializable())
      {
         try {
            obj = new MarshalledObject(obj);
         } catch (IOException e) {
            e.printStackTrace();
            throw new CacheException("Exception occurred during marshalling of non-serializable pojo: " +
            obj + " using JBoss Serialization", e);
         }
      } */
      // Note that we will only have one key in this fqn.
      map.put(InternalDelegate.SERIALIZED, obj);
      internal_.put(fqn, map);
   }

   protected boolean serializableObjectRemove(Fqn fqn)
      throws CacheException
   {
      // No need to do anything here since we will do clean up afterwards.
      return true;
   }

}
