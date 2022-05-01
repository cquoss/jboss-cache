/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.factories;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.TreeCache;
import org.jboss.cache.interceptors.Interceptor;
import org.jboss.cache.loader.CacheLoaderManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory class that builds an interceptor chain based on TreeCache config.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class InterceptorChainFactory
{
   private static Log log = LogFactory.getLog(InterceptorChainFactory.class);

   public Interceptor buildInterceptorChain(TreeCache cache) throws IllegalAccessException, ClassNotFoundException, InstantiationException
   {
      if (cache.isNodeLockingOptimistic())
      {
         return createOptimisticInterceptorChain(cache);
      }
      else
      {
         return createPessimisticInterceptorChain(cache);
      }
   }


   private Interceptor createInterceptor(String classname, TreeCache cache) throws ClassNotFoundException, IllegalAccessException, InstantiationException
   {
      Class clazz = loadClass(classname);
      Interceptor i = (Interceptor) clazz.newInstance();
      i.setCache(cache);
      return i;
   }

   /**
    * Adds an interceptor at the end of the chain
    */
   private void addInterceptor(Interceptor first, Interceptor i)
   {
      if (first == null) return;
      while (first.getNext() != null) first = first.getNext();
      first.setNext(i);
   }


   /**
    * Loads the specified class using this class's classloader, or, if it is <code>null</code>
    * (i.e. this class was loaded by the bootstrap classloader), the system classloader.
    * <p/>
    * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this
    * class may be loaded by the bootstrap classloader.
    * </p>
    *
    * @throws ClassNotFoundException
    */
   protected Class loadClass(String classname) throws ClassNotFoundException
   {
      ClassLoader cl = getClass().getClassLoader();
      if (cl == null)
         cl = ClassLoader.getSystemClassLoader();
      return cl.loadClass(classname);
   }


   private Interceptor createPessimisticInterceptorChain(TreeCache cache) throws IllegalAccessException, InstantiationException, ClassNotFoundException
   {
      Interceptor call_interceptor = null;
      Interceptor lock_interceptor = null;
      // Interceptor create_if_not_exists_interceptor=null;
      Interceptor repl_interceptor = null;
      Interceptor cache_loader_interceptor = null;
      Interceptor cache_store_interceptor = null;
      Interceptor unlock_interceptor = null;
      Interceptor passivation_interceptor = null;
      Interceptor activation_interceptor = null;
      Interceptor cacheMgmtInterceptor = null;
      Interceptor txInterceptor = null;
      Interceptor eviction_interceptor = null;
      Interceptor dataGravitatorInterceptor = null;
      Interceptor first = null;

      call_interceptor = createInterceptor("org.jboss.cache.interceptors.CallInterceptor", cache);

//      if (cache.getBuddyManager() != null && cache.getBuddyManager().isEnableDataGravitation()) dataGravitatorInterceptor = createInterceptor("org.jboss.cache.interceptors.DataGravitatorInterceptor", cache);
      if (cache.getBuddyManager() != null)
         dataGravitatorInterceptor = createInterceptor("org.jboss.cache.interceptors.DataGravitatorInterceptor", cache);

      lock_interceptor = createInterceptor("org.jboss.cache.interceptors.PessimisticLockInterceptor", cache);

      //create_if_not_exists_interceptor=createInterceptor("org.jboss.cache.interceptors.CreateIfNotExistsInterceptor");
      //create_if_not_exists_interceptor.setCache(this);

      unlock_interceptor = createInterceptor("org.jboss.cache.interceptors.UnlockInterceptor", cache);

      cacheMgmtInterceptor = createInterceptor("org.jboss.cache.interceptors.CacheMgmtInterceptor", cache);

      txInterceptor = createInterceptor("org.jboss.cache.interceptors.TxInterceptor", cache);

      switch (cache.getCacheModeInternal())
      {
         case TreeCache.REPL_SYNC:
         case TreeCache.REPL_ASYNC:
            repl_interceptor = createInterceptor("org.jboss.cache.interceptors.ReplicationInterceptor", cache);
//                if (!cache.isNodeLockingOptimistic()) cache.setReplicationHandler((Replicatable) repl_interceptor);
            break;
         case TreeCache.INVALIDATION_SYNC:
         case TreeCache.INVALIDATION_ASYNC:
            repl_interceptor = createInterceptor("org.jboss.cache.interceptors.InvalidationInterceptor", cache);
            break;
         case TreeCache.LOCAL:
            //Nothing...
      }

      CacheLoaderManager cacheLoaderMgr = cache.getCacheLoaderManager();

      if (cacheLoaderMgr != null && cacheLoaderMgr.getCacheLoader() != null)
      {
         if (cacheLoaderMgr.isPassivation())
         {
            activation_interceptor = createInterceptor("org.jboss.cache.interceptors.ActivationInterceptor", cache);
            passivation_interceptor = createInterceptor("org.jboss.cache.interceptors.PassivationInterceptor", cache);
         }
         else
         {
            cache_loader_interceptor = createInterceptor("org.jboss.cache.interceptors.CacheLoaderInterceptor", cache);
            cache_store_interceptor = createInterceptor("org.jboss.cache.interceptors.CacheStoreInterceptor", cache);
         }
      }

      // load the cache management interceptor first
      if (cache.getUseInterceptorMbeans())
      {
         if (first == null)
            first = cacheMgmtInterceptor;
         else
            addInterceptor(first, cacheMgmtInterceptor);
      }

      // load the tx interceptor
      if (first == null)
         first = txInterceptor;
      else
         addInterceptor(first, txInterceptor);

//      if (first == null)
//         first = lock_interceptor;
//      else
//         addInterceptor(first, lock_interceptor);

      // create the stack from the bottom up
      if (activation_interceptor != null)
      {
         if (!cacheLoaderMgr.isFetchPersistentState())
         {
            if (first == null)
               first = passivation_interceptor;
            else
               addInterceptor(first, passivation_interceptor);
         }
      }

      if (cache_loader_interceptor != null)
      {
         if (!cacheLoaderMgr.isFetchPersistentState())
         {
            if (first == null)
               first = cache_store_interceptor;
            else
               addInterceptor(first, cache_store_interceptor);
         }
      }

      if (repl_interceptor != null)
      {
         if (first == null)
            first = repl_interceptor;
         else
            addInterceptor(first, repl_interceptor);
      }

      if (unlock_interceptor != null)
      {
         if (first == null)
            first = unlock_interceptor;
         else
            addInterceptor(first, unlock_interceptor);
      }

      if (activation_interceptor != null)
      {
         if (!cacheLoaderMgr.isFetchPersistentState())
         {
            if (first == null)
               first = activation_interceptor;
            else
               addInterceptor(first, activation_interceptor);
         }
         else
         {
            if (first == null)
               first = activation_interceptor;
            else
               addInterceptor(first, activation_interceptor);
            if (first == null)
               first = passivation_interceptor;
            else
               addInterceptor(first, passivation_interceptor);
         }
      }

      if (cache_loader_interceptor != null)
      {
         if (!cacheLoaderMgr.isFetchPersistentState())
         {
            if (first == null)
               first = cache_loader_interceptor;
            else
               addInterceptor(first, cache_loader_interceptor);
         }
         else
         {
            if (first == null)
               first = cache_loader_interceptor;
            else
               addInterceptor(first, cache_loader_interceptor);
            if (first == null)
               first = cache_store_interceptor;
            else
               addInterceptor(first, cache_store_interceptor);
         }
      }

      if (dataGravitatorInterceptor != null)
      {
         if (first == null) first = dataGravitatorInterceptor;
         else addInterceptor(first, dataGravitatorInterceptor);
      }

      //if(first == null)
      // first=create_if_not_exists_interceptor;
      //else
      // addInterceptor(first, create_if_not_exists_interceptor);

      if (first == null)
         first = lock_interceptor;
      else
         addInterceptor(first, lock_interceptor);

      if (cache.isUsingEviction())
      {
         eviction_interceptor = createInterceptor(cache.getEvictionInterceptorClass(), cache);
         if (first == null)
            first = eviction_interceptor;
         else
            addInterceptor(first, eviction_interceptor);
      }

      if (first == null)
         first = call_interceptor;
      else
         addInterceptor(first, call_interceptor);

      if (log.isDebugEnabled())
         log.debug("interceptor chain is:\n" + printInterceptorChain(first));

      return first;
   }

   private Interceptor createOptimisticInterceptorChain(TreeCache cache) throws IllegalAccessException, InstantiationException, ClassNotFoundException
   {

      Interceptor txInterceptor = null, replicationInterceptor = null, lockInterceptor = null, validationInterceptor = null;
      Interceptor createIfNotExistsInterceptor = null, nodeInterceptor = null, invokerInterceptor = null, activationInterceptor = null;
      Interceptor passivationInterceptor = null, cacheLoaderInterceptor = null, cacheStoreInterceptor = null, first = null;
      Interceptor cacheMgmtInterceptor = null, evictionInterceptor = null, dataGravitatorInterceptor = null;

      CacheLoaderManager cacheLoaderManager = cache.getCacheLoaderManager();
      if (cacheLoaderManager != null && cacheLoaderManager.getCacheLoader() != null)
      {
         if (cacheLoaderManager.isPassivation())
         {
            activationInterceptor = createInterceptor("org.jboss.cache.interceptors.ActivationInterceptor", cache);
            passivationInterceptor = createInterceptor("org.jboss.cache.interceptors.PassivationInterceptor", cache);
         }
         else
         {
            cacheLoaderInterceptor = createInterceptor("org.jboss.cache.interceptors.CacheLoaderInterceptor", cache);
            cacheStoreInterceptor = createInterceptor("org.jboss.cache.interceptors.CacheStoreInterceptor", cache);
         }
      }

      txInterceptor = createInterceptor("org.jboss.cache.interceptors.TxInterceptor", cache);

//      if (cache.getBuddyManager() != null && cache.getBuddyManager().isEnableDataGravitation()) dataGravitatorInterceptor = createInterceptor("org.jboss.cache.interceptors.DataGravitatorInterceptor", cache);
      if (cache.getBuddyManager() != null)
         dataGravitatorInterceptor = createInterceptor("org.jboss.cache.interceptors.DataGravitatorInterceptor", cache);

      switch (cache.getCacheModeInternal())
      {
         case TreeCache.REPL_SYNC:
         case TreeCache.REPL_ASYNC:
            replicationInterceptor = createInterceptor("org.jboss.cache.interceptors.OptimisticReplicationInterceptor", cache);
//                if (!cache.isNodeLockingOptimistic()) cache.setReplicationHandler((Replicatable) replicationInterceptor);
            break;
         case TreeCache.INVALIDATION_SYNC:
         case TreeCache.INVALIDATION_ASYNC:
            replicationInterceptor = createInterceptor("org.jboss.cache.interceptors.InvalidationInterceptor", cache);
            break;
         case TreeCache.LOCAL:
            //Nothing...
      }

      lockInterceptor = createInterceptor("org.jboss.cache.interceptors.OptimisticLockingInterceptor", cache);

      validationInterceptor = createInterceptor("org.jboss.cache.interceptors.OptimisticValidatorInterceptor", cache);

      createIfNotExistsInterceptor = createInterceptor("org.jboss.cache.interceptors.OptimisticCreateIfNotExistsInterceptor", cache);

      nodeInterceptor = createInterceptor("org.jboss.cache.interceptors.OptimisticNodeInterceptor", cache);

      invokerInterceptor = createInterceptor("org.jboss.cache.interceptors.CallInterceptor", cache);

      if (cache.isUsingEviction())
      {
         evictionInterceptor = createInterceptor(cache.getEvictionInterceptorClass(), cache);
      }

      if (cache.getUseInterceptorMbeans())
      {
         cacheMgmtInterceptor = createInterceptor("org.jboss.cache.interceptors.CacheMgmtInterceptor", cache);
         if (first == null)
         {
            first = cacheMgmtInterceptor;
         }
         else
         {
            addInterceptor(first, cacheMgmtInterceptor);
         }
      }

      if (txInterceptor != null)
      {
         if (first == null)
         {
            first = txInterceptor;
         }
         else
         {
            addInterceptor(first, txInterceptor);
         }
      }

      if (first == null)
      {
         first = replicationInterceptor;
      }
      else
      {
         addInterceptor(first, replicationInterceptor);
      }

      if (passivationInterceptor != null && !cacheLoaderManager.isFetchPersistentState())
      {
         if (first == null)
            first = passivationInterceptor;
         else
            addInterceptor(first, passivationInterceptor);
      }

      // add the cache store interceptor here
      if (cacheStoreInterceptor != null && !cacheLoaderManager.isFetchPersistentState())
      {
         if (first == null)
            first = cacheStoreInterceptor;
         else
            addInterceptor(first, cacheStoreInterceptor);
      }

      // cache loader interceptor is only invoked if we are ready to write to the actual tree cache
      if (activationInterceptor != null)
      {
         if (first == null)
            first = activationInterceptor;
         else
            addInterceptor(first, activationInterceptor);

         if (cacheLoaderManager.isFetchPersistentState())
         {
            if (first == null)
               first = passivationInterceptor;
            else
               addInterceptor(first, passivationInterceptor);
         }
      }

      if (cacheLoaderInterceptor != null)
      {
         if (first == null)
            first = cacheLoaderInterceptor;
         else
            addInterceptor(first, cacheLoaderInterceptor);

         if (cacheLoaderManager.isFetchPersistentState())
         {
            if (first == null)
               first = cacheStoreInterceptor;
            else
               addInterceptor(first, cacheStoreInterceptor);
         }
      }

      if (dataGravitatorInterceptor != null)
      {
         if (first == null) first = dataGravitatorInterceptor;
         else addInterceptor(first, dataGravitatorInterceptor);
      }


      if (first == null)
      {
         first = lockInterceptor;
      }
      else
      {
         addInterceptor(first, lockInterceptor);
      }

      if (first == null)
      {
         first = validationInterceptor;
      }
      else
      {
         addInterceptor(first, validationInterceptor);
      }

      if (first == null)
      {
         first = createIfNotExistsInterceptor;
      }
      else
      {
         addInterceptor(first, createIfNotExistsInterceptor);
      }

      // eviction interceptor to come before the optimistic node interceptor 
      if (first == null)
      {
         first = evictionInterceptor;
      }
      else
      {
         addInterceptor(first, evictionInterceptor);
      }

      if (first == null)
      {
         first = nodeInterceptor;
      }
      else
      {
         addInterceptor(first, nodeInterceptor);
      }


      if (first == null)
      {
         first = invokerInterceptor;
      }
      else
      {
         addInterceptor(first, invokerInterceptor);
      }

      if (log.isInfoEnabled())
         log.info("interceptor chain is:\n" + printInterceptorChain(first));

      return first;
   }

   public static String printInterceptorChain(Interceptor i)
   {
      StringBuffer sb = new StringBuffer();
      if (i != null)
      {
         if (i.getNext() != null)
         {
            sb.append(printInterceptorChain(i.getNext())).append("\n");
         }
         sb.append(i.getClass());
      }
      return sb.toString();
   }

   public static List asList(Interceptor interceptor)
   {
      if (interceptor == null)
         return null;
      int num = 1;
      Interceptor tmp = interceptor;
      while ((tmp = tmp.getNext()) != null)
      {
         num++;
      }
      List retval = new ArrayList(num);
      tmp = interceptor;
      num = 0;
      do
      {
         retval.add(tmp);
         tmp = tmp.getNext();
      }
      while (tmp != null);
      return retval;
   }
}
