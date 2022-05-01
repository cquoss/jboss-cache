package org.jboss.cache.aop;

import java.lang.reflect.Method;

/**
 * Class represent a class attribute. Currently not in use.
 * @author <a href="mailto:harald@gliebe.de">Harald Gliebe</a>
 * @author Ben Wang
 */

public class CachedAttribute
{

   protected String name;
   protected Class type;
   protected Method get, set;

   public CachedAttribute()
   {
   }

   public CachedAttribute(String name)
   {
      this.name = name;
   }

   public String getName()
   {
      return this.name;
   }

   public Class getType()
   {
      return this.type;
   }

   public void setType(Class type)
   {
      if (this.type != null && this.type != type) {
         // TODO: provide better info
         throw new IllegalArgumentException("get/set types differ");
      }
      this.type = type;
   }

   public Method getGet()
   {
      return this.get;
   }

   public void setGet(Method get)
   {
      this.get = get;
   }

   public Method getSet()
   {
      return this.set;
   }

   public void setSet(Method set)
   {
      this.set = set;
   }

} // CachedAttribute
