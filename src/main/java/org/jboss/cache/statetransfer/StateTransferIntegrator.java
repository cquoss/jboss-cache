/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import org.jboss.cache.DataNode;

public interface StateTransferIntegrator
{

   void integrateTransientState(DataNode target, ClassLoader cl) throws Exception;

   void integratePersistentState() throws Exception;

}