/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import org.jboss.cache.DataNode;

public interface StateTransferGenerator
{

   public abstract byte[] generateStateTransfer(DataNode rootNode, boolean generateTransient,
         boolean generatePersistent, boolean suppressErrors) throws Throwable;

}