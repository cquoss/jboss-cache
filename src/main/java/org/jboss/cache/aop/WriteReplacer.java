/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop;

//import java.util.Iterator;
//import java.util.List;

import org.jboss.aop.Advised;
import org.jboss.aop.InstanceAdvisor;

import java.io.NotSerializableException;
import java.io.ObjectStreamException;

/**
 * @author <a href="mailto:harald@gliebe.de">Harald Gliebe</a>
 * @deprecated 1.0
 */

public class WriteReplacer implements WriteReplaceable /*, Interceptor*/
{

   // interceptor
   /*
       public WriteReplacer() {
       }

       public String getName() {
      return "WriteReplacer";
       }

       public InvocationResponse invoke(Invocation invocation) throws Throwable {
      Method method = MethodInvocation.getMethod(invocation);

      if (method != null
          && method.getName().equals("writeReplace")
          && method.getReturnType().equals(Object.class)
          && method.getParameterTypes().length == 0) {
          //hack
          this.obj = MethodInvocation.getTargetObject(invocation);
          this.writeReplace(); // fills Fieldvalues
      }

      return invocation.invokeNext();

       }
   */
   // mixin

   Object obj;

   public WriteReplacer(Object obj)
   {
      this.obj = obj;
   }

   public Object writeReplace() throws ObjectStreamException
   {
      if (obj instanceof Advised) {
         InstanceAdvisor advisor = ((Advised) obj)._getInstanceAdvisor();
         org.jboss.aop.advice.Interceptor[] interceptors = advisor.getInterceptors();
         CacheInterceptor cacheInterceptor = null;
         for (int i = 0; i < interceptors.length; i++) {
            if (interceptors[i] instanceof CacheInterceptor) {
               cacheInterceptor = (CacheInterceptor) interceptors[i];
               break;
            }
         }
         if (cacheInterceptor != null) {
            try {
               cacheInterceptor.beforeSerialization(obj);
            } catch (Exception e) {
               e.printStackTrace();
               throw new NotSerializableException(e.getMessage());
            }
         }
      }
      return obj;
   }

}
