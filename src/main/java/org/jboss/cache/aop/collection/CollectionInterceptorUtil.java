/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.aop.InstanceAdvisor;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.proxy.ClassProxy;
import org.jboss.aop.proxy.ClassProxyFactory;
import org.jboss.aop.util.MethodHashing;
import org.jboss.cache.Fqn;
import org.jboss.cache.aop.PojoCache;
import org.jboss.cache.aop.util.AopUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;

/**
 * CollectionInterceptorUtil contains helper methods for the interceptors of
 * the different collection types.
 *
 * @author <a href="mailto:harald@gliebe.de">Harald Gliebe</a>
 * @author Ben Wang
 */
public class CollectionInterceptorUtil
{
   static Log log=LogFactory.getLog(CollectionInterceptorUtil.class.getName());

   public static ClassProxy createProxy(Class clazz, AbstractCollectionInterceptor interceptor)
         throws Exception
   {
      ClassProxy result = ClassProxyFactory.newInstance(clazz);
      InstanceAdvisor advisor = result._getInstanceAdvisor();
      advisor.appendInterceptor(interceptor);
      return result;
   }

   public static ClassProxy createMapProxy(PojoCache cache, Fqn fqn, Class clazz, Map obj) throws Exception {
      return CollectionInterceptorUtil.createProxy(clazz, new CachedMapInterceptor(cache, fqn, clazz, obj));
   }

   public static ClassProxy createListProxy(PojoCache cache, Fqn fqn, Class clazz, List obj) throws Exception {
      return CollectionInterceptorUtil.createProxy(clazz, new CachedListInterceptor(cache, fqn, clazz, obj));
   }

   public static ClassProxy createSetProxy(PojoCache cache, Fqn fqn, Class clazz, Set obj) throws Exception {
      return CollectionInterceptorUtil.createProxy(clazz, new CachedSetInterceptor(cache, fqn, clazz, obj));
   }

   public static AbstractCollectionInterceptor getInterceptor(ClassProxy proxy)
   {
      InstanceAdvisor advisor = proxy._getInstanceAdvisor();
      return (AbstractCollectionInterceptor)AopUtil.findCollectionInterceptor(advisor);
   }

   public static Map getMethodMap(Class clazz)
   {
      Map result = ClassProxyFactory.getMethodMap(clazz.getName());
      if (result == null) {
         try {
            ClassProxyFactory.newInstance(clazz);
         } catch (RuntimeException re) {
            throw re;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
         result = ClassProxyFactory.getMethodMap(clazz.getName());
      }
      return result;
   }

   public static Map getManagedMethods(Class clazz)
   {
      Method tostring = null;
      try {
         tostring = Object.class.getDeclaredMethod("toString", new Class[0]);
      } catch (NoSuchMethodException e) {
         e.printStackTrace();
         throw new RuntimeException("getManagedMathods: " +e);
      }

      Map managedMethods = new HashMap();
      try {
         Method[] methods = clazz.getDeclaredMethods();
         for (int i = 0; i < methods.length; i++) {
            long hash = MethodHashing.methodHash(methods[i]);
            managedMethods.put(new Long(hash), methods[i]);
         }
         // Add toString to ManagedMethod
         long hash = MethodHashing.methodHash(tostring);
         managedMethods.put(new Long(hash), tostring);
      } catch (Exception ignored) {
         ignored.printStackTrace();
      }
      return managedMethods;
   }

   public static Object invoke(Invocation invocation,
                               Object interceptor,
                               Map methodMap,
                               Map managedMethods)
         throws Throwable
   {

      try {
         if (invocation instanceof MethodInvocation) {
            MethodInvocation methodInvocation = (MethodInvocation) invocation;
            Long methodHash = new Long(methodInvocation.getMethodHash());
            Method method = (Method) managedMethods.get(methodHash);
            if (log.isDebugEnabled() && method != null) {
               log.trace("invoke(): method intercepted " + method.getName());
            }
            Object[] args = methodInvocation.getArguments();
            if (method != null) {
               return method.invoke(interceptor, args);
            } else {
               method = methodInvocation.getMethod();
               if (method == null) {
                  method = (Method) methodMap.get(methodHash);
               }

               log.trace("invke(): invoke non-managed method: " +method.toString());
               Object target = methodInvocation.getTargetObject();
               if(target == null) {
                  throw new RuntimeException("CollectionInterceptorUtil.invoke(): targetObject is null." +
                        " Can't invoke " +method.toString());
               }
               return method.invoke(target, args);
   //            return method.invoke(interceptor, args);
            }
         }
      }
      catch(InvocationTargetException e) {
         if(e.getCause() != null)
            throw e.getCause();
         else if(e.getTargetException() != null)
            throw e.getTargetException();
         throw e;
      }

      return invocation.invokeNext();
   }

}
