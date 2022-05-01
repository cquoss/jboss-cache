/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.marshall;

import org.jgroups.blocks.MethodCall;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;

/**
 * A subclass of the JGroups MethodCall class, which adds a method id.
 * <p/>
 * NB: The reason for not using JGroups' MethodCall class directly, which does contain an id property
 * since JGroups 2.2.9, is for backward compatibility with JGroups 2.2.7 and 2.2.8.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class JBCMethodCall extends MethodCall
{
   private int methodId;
   private static final long serialVersionUID = -4826713878871338199L;
   private static boolean trace = LogFactory.getLog(JBCMethodCall.class).isTraceEnabled();

   public JBCMethodCall()
   {
   }

   public JBCMethodCall(Method method, Object[] arguments, int methodId)
   {
      super(method, arguments);
      this.methodId = methodId;
   }

   public int getMethodId()
   {
      return methodId;
   }

   public void setMethodId(int methodId)
   {
      this.methodId = methodId;
   }

   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final JBCMethodCall that = (JBCMethodCall) o;

      return (methodId == that.methodId) && super.equals(o);
   }

   public int hashCode()
   {
      return super.hashCode() * 10 + methodId;
   }

   public String toString()
   {
      StringBuffer ret = new StringBuffer();
      ret.append(method_name);
      ret.append("; id:");
      ret.append(methodId);
      ret.append("; Args: (");
      if (args != null && args.length > 0)
      {
         if (trace)
         {
            boolean first = true;
            for (int i=0; i<args.length; i++)
            {
               if (first) first = false;
               else ret.append(", ");
               ret.append(args[i]);
            }
         }
         else
         {
            ret.append(" arg[0] = ");
            ret.append(args[0]);
            if (args.length > 1) ret.append(" ...");
         }
      }
      ret.append(')');
      return ret.toString();
   }
}
