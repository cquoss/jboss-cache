package org.jboss.cache.aop;

import org.jboss.aop.joinpoint.FieldInvocation;
import org.jboss.aop.Advisor;
import org.jboss.cache.aop.references.FieldPersistentReference;
import org.jboss.cache.aop.references.PersistentReference;
import org.jboss.cache.aop.annotation.Transient;
import org.jboss.cache.aop.annotation.Serializable;
import org.jboss.cache.aop.annotation.NonTransient;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

/**  Represent a cached object type, e.g., whether it is <b>primitive</b> or not.
 * Note: need to pay special attention not to leak classloader.
 * @author <a href="mailto:harald@gliebe.de">Harald Gliebe</a>
 * @author Ben Wang
 */

public class CachedType
{
   // Types that are considered "primitive".
   protected static final Set immediates =
         new HashSet(Arrays.asList(new Object[]{
            String.class,
            Boolean.class,
            Double.class,
            Float.class,
            Integer.class,
            Long.class,
            Short.class,
            Character.class,
            Boolean.TYPE,
            Double.TYPE,
            Float.TYPE,
            Integer.TYPE,
            Long.TYPE,
            Short.TYPE,
            Character.TYPE,
            Class.class}));

   protected WeakReference type;
   protected boolean immutable;
   protected boolean immediate;
   // This map caches the class that contains no annotation.
   protected static Map CachedClassWithNoAnnotation_ = new WeakHashMap();
   protected static Map CachedClassWithAnnotation_ = new WeakHashMap();

   // Java fields . Will use special FieldPersistentReference to prevent classloader leakage.
   protected List fields = new ArrayList();
   protected Map fieldMap = new HashMap(); // Name -> CachedAttribute

   public CachedType()
   {
   }

   public CachedType(Class type)
   {
      this.type = new WeakReference(type);
      analyze();
   }

   public Class getType()
   {
      return (Class)type.get();
   }

   // determines if the object should be stored in the Nodes map or as a subnode
   public boolean isImmediate()
   {
      return immediate;
   }

   public static boolean isImmediate(Class clazz)
   {
      return immediates.contains(clazz);
   }

   public boolean isImmutable()
   {
      return immutable;
   }

   /**
    * A <code>List<Field></code> of all of this type's replicatable
    * fields.
    *
    * @see #isStaticOrFinalField(Field)
    */
   public List getFields()
   {
      if (fields == null)
         return null;

      // Make a defensive copy that hides the use of
      // FieldPersistentReference
      ArrayList result = new ArrayList(fields.size());

      for (Iterator iter = getFieldsIterator(); iter.hasNext();)
         result.add(iter.next());

      return result;
   }

   /**
    * Returns an iterator over this CachedType's fields.  If an iterator
    * is all that is wanted, this is more efficient than calling
    * <code>getFields().iterator()</code>, as it saves the step of making a
    * defensive copy of the fields list.
    *
    * Note that the iterator does not support <code>remove()</code>
    */
   public Iterator getFieldsIterator()
   {
      return new FieldsIterator(this);
   }

   public Field getField(String name)
   {
      FieldPersistentReference ref = (FieldPersistentReference)fieldMap.get(name);
      if (ref==null) return null;
      return (Field)ref.get();
   }

   /*
   public List getAttributes()
   {
      return attributes;
   }

   public CachedAttribute getAttribute(Method m)
   {
      return (CachedAttribute) attributeMap.get(m);
   }

   protected void setAttributes(List attributes)
   {
      this.attributes = attributes;

      attributeMap.clear();

      // TODO: is a class with no set methods immutable ?
      this.immutable = true;

      for (Iterator i = attributes.iterator(); i.hasNext();) {
         CachedAttribute attribute = (CachedAttribute) i.next();
         if (attribute.getGet() != null) {
            attributeMap.put(attribute.getGet(), attribute);
         }
         if (attribute.getSet() != null) {
            attributeMap.put(attribute.getSet(), attribute);
            immutable = false;
         }
      }
   }
   */

   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append(getType().getName()).append(" {\n");
      /*
      for (Iterator i = attributes.iterator(); i.hasNext();) {
         CachedAttribute attr = (CachedAttribute) i.next();
         sb
               .append("\t")
               .append(attr.getType().getName())
               .append(" ")
               .append(attr.getName())
               .append(" [")
               .append(attr.getGet() == null
               ? "<no get>"
               : attr.getGet().getName())
               .append(", ")
               .append(attr.getSet() == null
               ? "<no set>"
               : attr.getSet().getName())
               .append("]\n");
      }
      */
      sb.append("}, immutable =").append(immutable);
      return sb.toString();
   }

   /* ---------------------------------------- */

   private void analyze()
   {

      /*
      // We intercept all fields now (instead of setter methods) so there is no need to
      // track the individual fields.
      HashMap attributes = new HashMap();
      Method[] methods = type.getMethods();
      for (int i = 0; i < methods.length; i++) {
         Method method = methods[i];
         if (isGet(method)) {
            CachedAttribute attribute =
                  getAttribute(method, attributes, true);
            attribute.setGet(method);
            attribute.setType(method.getReturnType());
         } else if (isSet(method)) {
            CachedAttribute attribute =
                  getAttribute(method, attributes, true);
            attribute.setSet(method);
            attribute.setType(method.getParameterTypes()[0]);
         }
      }
      this.setAttributes(new ArrayList(attributes.values()));
      */
      analyzeFields(getType());

      immediate = isImmediate(getType());

   }

   void analyzeFields(Class clazz)
   {
      if (clazz == null)
         return;

      analyzeFields(clazz.getSuperclass());

      Field[] classFields = clazz.getDeclaredFields();
      for (int i = 0; i < classFields.length; i++) {
         Field f = classFields[i];
         if(isStaticOrFinalField(f)) continue;

         f.setAccessible(true);

         FieldPersistentReference persistentRef = new FieldPersistentReference(f, PersistentReference.REFERENCE_SOFT);

         fields.add(persistentRef);
         fieldMap.put(f.getName(), persistentRef);
      }
   }

   /**
    * We check whether this class has any field annotation declaration. We assume that there is only
    * one such declaring class per vm and it is static.
    */
   protected static boolean hasAnnotation(Class clazz, Advisor advisor, CachedType type)
   {
      // It is ok that we don't synchronize it here.
      if(CachedClassWithNoAnnotation_.get(clazz) != null)
      {
         return false;
      } else if (CachedClassWithAnnotation_.get(clazz) != null)
      {
         return true;
      }

      for (Iterator i = type.getFieldsIterator(); i.hasNext();) {
         Field field = (Field) i.next();
         // check for non-replicatable types
         if(CachedType.hasFieldAnnotation(field, advisor))
         {
            synchronized(CachedClassWithAnnotation_)
            {
               CachedClassWithAnnotation_.put(clazz, clazz.getName());
            }
            return true;
         }
      }

      // This obj class doesn't have field annotation. It is ok that multiple threads
      // put it repeatedly actually.
      synchronized(CachedClassWithNoAnnotation_)
      {
         CachedClassWithNoAnnotation_.put(clazz, clazz.getName());
      }
      return false;
   }

   protected static boolean isStaticOrFinalField(Field f) {
      int mods = f.getModifiers();
      /**
       * The following modifiers are ignored in the cache, i.e., they will not be stored in the cache.
       * Whenever, user trying to access these fields, it will be accessed from the in-memory version.
       */
      if (Modifier.isStatic(mods)
//            || Modifier.isTransient(mods)
            || Modifier.isFinal(mods)) {
         return true;
      }
      return false;
   }

   protected static boolean isTransientField(Field f) {
      int mods = f.getModifiers();
      /**
       * The following modifiers are ignored in the cache, i.e., they will not be stored in the cache.
       * Whenever, user trying to access these fields, it will be accessed from the in-memory version.
       */
      if (Modifier.isTransient(mods)) {
         return true;
      }
      return false;
   }


   /**
    * Check if we have @Transient annotation.
    * @param invocation
    * @return true if @Transient is present.
    */
   protected static boolean hasTransientAnnotation(FieldInvocation invocation)
   {
      Object obj = invocation.resolveAnnotation(Transient.class);
      if(obj != null)
      {
         return true;
      }
      return false;
   }

   /**
    * Check if we have @NonTransient annotation.
    * @param invocation
    * @return true if @NonTransient is present.
    */
   protected static boolean hasNonTransientAnnotation(FieldInvocation invocation)
   {
      Object obj = invocation.resolveAnnotation(NonTransient.class);
      if(obj != null)
      {
         return true;
      }
      return false;
   }

   protected static boolean hasFieldAnnotation(Field field, Advisor advisor)
   {
      return hasTransientAnnotation(field, advisor) || hasSerializableAnnotation(field, advisor)
              || hasNonTransientAnnotation(field, advisor);
   }

   protected static boolean hasTransientAnnotation(Field field, Advisor advisor)
   {
      Object obj = advisor.resolveAnnotation(field, Transient.class);
      if(obj != null)
      {
         return true;
      }
      return false;
   }

   protected static boolean hasNonTransientAnnotation(Field field, Advisor advisor)
   {
      Object obj = advisor.resolveAnnotation(field, NonTransient.class);
      if(obj != null)
      {
         return true;
      }
      return false;
   }

   public static boolean hasSerializableAnnotation(Field field, Advisor advisor)
   {
      Object obj = advisor.resolveAnnotation(field, Serializable.class);
      if(obj != null)
      {
         return true;
      }
      return false;
   }

   public static boolean hasSerializableAnnotation(FieldInvocation invocation)
   {
      Object obj = invocation.resolveAnnotation(Serializable.class);
      if(obj != null)
      {
         return true;
      }
      return false;
   }

   public static boolean isNonReplicatable(FieldInvocation fieldInvocation)
   {
      return hasTransientAnnotation(fieldInvocation)
              || isStaticOrFinalField(fieldInvocation.getField())
              || (isTransientField(fieldInvocation.getField()) &&
                  !hasNonTransientAnnotation(fieldInvocation));
   }

   public static boolean isNonReplicatable(Field field, Advisor advisor)
   {
      return hasTransientAnnotation(field, advisor)
              || isStaticOrFinalField(field)
              || (isTransientField(field) &&
                  !hasNonTransientAnnotation(field, advisor));
   }


   /*
    * converts a get/set method to an attribute name
    */
   protected String attributeName(String methodName)
   {
      return methodName.substring(3, 4).toLowerCase()
            + methodName.substring(4);
   }

   protected CachedAttribute getAttribute(Method method,
                                          Map map,
                                          boolean create)
   {
      String name = attributeName(method.getName());

      CachedAttribute attribute = (CachedAttribute) map.get(name);
      if (create && attribute == null) {
         attribute = new CachedAttribute(name);
         map.put(name, attribute);
      }
      return attribute;
   }

   protected boolean isGet(Method method)
   {
      return method.getName().startsWith("get")
            && method.getParameterTypes().length == 0
            && method.getReturnType() != Void.TYPE;
   }

   protected boolean isSet(Method method)
   {
      return method.getName().startsWith("set")
            && method.getParameterTypes().length == 1
            && method.getReturnType() == Void.TYPE;
   }

   /**
    * Simple wrapper around the iterator for the CachedType.fields member
    * that
    *
    * @author <a href="brian.stansberry@jboss.com">Brian Stansberry</a>
    * @version $Id: CachedType.java 2377 2006-08-16 07:11:51Z bwang $
    */
   private class FieldsIterator implements Iterator
   {
      private Iterator source;
      private Class clazz;

      FieldsIterator(CachedType type)
      {
         // Hold a ref to the class so it doesn't get gc'd
         // while the iterator is alive
         clazz = type.getType();
         if (clazz == null)
            throw new RuntimeException("Class was already unloaded");

         source = type.fields.iterator();
      }

      public boolean hasNext()
      {
         return source.hasNext();
      }

      public Object next()
      {
         FieldPersistentReference ref = (FieldPersistentReference) source.next();
         return (ref == null) ? null : ref.get();
      }

      /**
       * Always throws <code>UnsupportedOperationException</code> as
       * removing fields from a CachedType is not valid.
       */
      public void remove()
      {
         throw new UnsupportedOperationException("Cannot remove a field from a CachedType");
      }
   }

} // CachedType
