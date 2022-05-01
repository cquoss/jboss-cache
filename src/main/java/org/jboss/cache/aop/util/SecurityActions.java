/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jboss.cache.aop.util;

import java.lang.reflect.AccessibleObject;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Copied from aop module.
 * @version $Revision: 2340 $
 */
public class SecurityActions
{
   interface SetAccessibleAction
   {
      void setAccessible(AccessibleObject accessibleObject);

      SetAccessibleAction PRIVILEGED = new SetAccessibleAction()
      {
         public void setAccessible(final AccessibleObject accessibleObject)
         {
            try
            {
               AccessController.doPrivileged(new PrivilegedExceptionAction()
               {
                  public Object run() throws Exception
                  {
                     accessibleObject.setAccessible(true);
                     return null;
                  }
               });
            }
            catch (PrivilegedActionException e)
            {
               throw new RuntimeException("Error setting " + accessibleObject + " as accessible ", e.getException());
            }
         }
      };

      SetAccessibleAction NON_PRIVILEGED = new SetAccessibleAction()
      {
         public void setAccessible(AccessibleObject accessibleObject)
         {
            accessibleObject.setAccessible(true);
         }
      };
   }

   public static void setAccessible(AccessibleObject accessibleObject)
   {
      if (System.getSecurityManager() == null)
      {
         SetAccessibleAction.NON_PRIVILEGED.setAccessible(accessibleObject);
      }
      else
      {
         SetAccessibleAction.PRIVILEGED.setAccessible(accessibleObject);
      }
   }

}
