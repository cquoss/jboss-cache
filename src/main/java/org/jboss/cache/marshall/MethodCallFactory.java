/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.marshall;

import java.lang.reflect.Method;

/**
 * Factory class to create instances of org.jgroups.blocks.MethodCall
 *
 * @author <a href="galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @version $Revision: 2045 $
 */
public class MethodCallFactory
{
    /**
     * Creates and initialised an instance of MethodCall
     *
     * @param method    Method instance of the MethodCall
     * @param arguments list of parameters
     * @return a new instance of MethodCall with the method id initialised
     */
    public static JBCMethodCall create(Method method, Object[] arguments)
    {
        JBCMethodCall mc = new JBCMethodCall(method, arguments, MethodDeclarations.lookupMethodId(method));
        return mc;
    }
}
