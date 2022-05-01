/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.aop.joinpoint.FieldReadInvocation;
import org.jboss.aop.joinpoint.FieldWriteInvocation;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.joinpoint.FieldInvocation;
import org.jboss.aop.Advised;
import org.jboss.aop.InstanceAdvisor;
import org.jboss.aop.Advisor;
import org.jboss.cache.Fqn;
import org.jboss.cache.aop.util.AopUtil;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;

/**
 * Created: Sat Apr 26 10:35:01 2003
 *
 * @author Harald Gliebe
 * @author Ben Wang
 */

public class CacheInterceptor implements BaseInterceptor
{
   protected static final Log log_ = LogFactory.getLog(CacheInterceptor.class);
   protected PojoCache cache;
   protected CachedType type;
   protected Fqn fqn;
   boolean checkSerialization;
   protected String name;
   protected AOPInstance aopInstance;

   static Method writeExternal, readExternal;

   static
   {
      try {
         writeExternal =
               Externalizable.class.getMethod("writeExternal",
                     new Class[]{ObjectOutput.class});
         readExternal =
               Externalizable.class.getMethod("readExternal",
                     new Class[]{ObjectInput.class});
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public CacheInterceptor(PojoCache cache, Fqn fqn, CachedType type)
   {
      this.cache = cache;
      this.fqn = fqn;
      this.type = type;
      checkSerialization =
            !WriteReplaceable.class.isAssignableFrom(type.getType());
   }

   public AOPInstance getAopInstance() {
      return aopInstance;
   }

   public void setAopInstance(AOPInstance aopInstance) {
      this.aopInstance = aopInstance;
   }

   public String getName()
   {
      if(name == null)
      {
         this.name = "CacheInterceptor on [" + fqn + "]";
      }
      return name;
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      // Check if CLASS_INTERNAL exists. If not, that means we are done. We need to remove ourself.
      // Note that if speed is important, we will need to perform the detach step pro-actively,
      // that is, use a listener to listen for the removeObject event.
      if(isPojoDetached(invocation))
      {
         return invocation.invokeNext();  // invoke the in-memory pojo directly
      }

      if (invocation instanceof FieldWriteInvocation) {
         FieldInvocation fieldInvocation =
               (FieldInvocation) invocation;

         Advisor advisor = fieldInvocation.getAdvisor();
         Field field = fieldInvocation.getField();

         // Only if this field is replicatable. static, transient and final are not.
         CachedType fieldType = cache.getCachedType(field.getType());
         CachedType parentType = cache.getCachedType(field.getDeclaringClass());
         if(!isNonReplicatable(fieldInvocation)) {
            Object value = ((FieldWriteInvocation)fieldInvocation).getValue();
            if (fieldType.isImmediate() || hasSerializableAnnotation(field, advisor, parentType)) {
               cache.put(fqn, field.getName(), value);
            } else {
               //cache.putObject(((Fqn)fqn.clone()).add(field.getName()), value);
               cache.putObject(new Fqn(fqn, field.getName()), value);
            }
         }

      } else if (invocation instanceof FieldReadInvocation) {
         FieldInvocation fieldInvocation =
               (FieldInvocation) invocation;
         Field field = fieldInvocation.getField();
         Advisor advisor = fieldInvocation.getAdvisor();

         // Only if this field is replicatable
         CachedType fieldType = cache.getCachedType(field.getType());
         CachedType parentType = cache.getCachedType(field.getDeclaringClass());
         if( !isNonReplicatable(fieldInvocation)) {
            Object result;
            if (fieldType.isImmediate()|| hasSerializableAnnotation(field, advisor, parentType)) {
               result = cache.get(fqn, field.getName());
            } else {
               //result = cache.getObject(((Fqn)fqn.clone()).add(field.getName()));
               result = cache.getObject(new Fqn(fqn, field.getName()));
            }

            // if result is null, we need to make sure the in-memory reference is null
            // as well. If it is not, then we know this one is null because it has
            // been evicted. Will need to reconstruct it
            if(result != null)
               return result;
            else {
               // TODO There is a chance of recursive loop here if caller tries to print out obj that will trigger the fieldRead interception.
               Object value = invocation.getTargetObject();
               if(value == null || field.get(value) == null)   // if both are null, we know this is null as well.
                  return null;
               else {
                  if(log_.isTraceEnabled()) {
                     log_.trace("invoke(): DataNode on fqn: " +fqn + " has obviously been evicted. Will need to reconstruct it");
                  }

                  cache.putObject(fqn, value);
               }
            }
         }
      } else if (checkSerialization) { // Have no use now.
         MethodInvocation methodInvocation = (MethodInvocation) invocation;
         Method method = methodInvocation.getMethod();

         if (method != null
               && method.getName().equals("writeReplace")
               && method.getReturnType().equals(Object.class)
               && method.getParameterTypes().length == 0) {

            beforeSerialization(invocation.getTargetObject());
         } else if (method == writeExternal) {
            Object target = methodInvocation.getTargetObject();
            beforeSerialization(target);
         }
      }

      return invocation.invokeNext();

   }

   /**
    * See if this field is non-replicatable such as @Transient or transient modifier.
    */
   private boolean isNonReplicatable(FieldInvocation fieldInvocation)
   {
      return CachedType.isNonReplicatable(fieldInvocation);
   }

   private boolean hasSerializableAnnotation(Field field, Advisor advisor, CachedType type)
   {
      if(CachedType.hasAnnotation(field.getDeclaringClass(), advisor, type))
      {
         if(CachedType.hasSerializableAnnotation(field, advisor)) return true;
      }

      return false;
   }

   /**
    * Check if the pojo is detached already. If it is and we still have the cache interceptor on
    * this pojo, we will go ahead and remove it since it should not be there in the first place.
    * @param invocation
    * @return
    * @throws Exception
    */
   protected boolean isPojoDetached(Invocation invocation) throws Exception
   {
      boolean detached = false;
      if( !cache.exists(fqn, InternalDelegate.CLASS_INTERNAL) )
      {
         detached = true;
         Object obj = invocation.getTargetObject();
         if(! (obj instanceof Advised) )
            throw new RuntimeException("Interception on non-advised pojo " +obj.toString());

         InstanceAdvisor advisor = ((Advised)obj)._getInstanceAdvisor();
         CacheInterceptor interceptor = (CacheInterceptor) AopUtil.findCacheInterceptor(advisor);
         if (interceptor != null)
         {
            if (log_.isDebugEnabled()) {
               log_.debug("isPojoDetached(): removed cache interceptor fqn: " + fqn + " interceptor: "+interceptor);
            }
            advisor.removeInterceptor(interceptor.getName());
         }
      }

      return detached;
   }

   protected void checkCacheConsistency() throws Exception
   {
      if (this != cache.peek(fqn, AOPInstance.KEY)) {
         throw new RuntimeException("Cache inconsistency: Outdated AOPInstance");
      }
   }

   public void beforeSerialization(Object target) throws Exception
   {

      // fill objects
      for (Iterator i = type.getFieldsIterator(); i.hasNext();) {
         Field field = (Field) i.next();
         CachedType fieldType = cache.getCachedType(field.getType());
         Object value = null;
         if (fieldType.isImmediate()) {
            value = cache.get(fqn, field.getName());
         } else {
            //		value = removeObject(fqn+TreeCache.SEPARATOR+field.getName());
            //value = cache.getObject(((Fqn)fqn.clone()).add(field.getName()));
            value = cache.getObject(new Fqn(fqn, field.getName()));
         }
         //	    System.out.println("Setting field " + field.getName() + "[" + field.getDeclaringClass() + "] of "+ target.getClass() + " to " + value);
         field.set(target, value);
      }
   }

   boolean isChildOf(Fqn parentFqn)
   {
      return fqn.isChildOf(parentFqn);
   }

//   void setFqn(Fqn fqn)
//   {
//      this.fqn = fqn;
//   }

   public Fqn getFqn() {
      return fqn;
   }

   public void setFqn(Fqn fqn)
   {
      this.fqn = fqn;
   }

}
