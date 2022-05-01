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
package org.jboss.cache.interceptors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.InvocationContext;
import org.jboss.cache.TreeCache;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jgroups.blocks.MethodCall;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing an interceptor.
 * <em>Note that this will be replaced by {@link org.jboss.aop.advice.Interceptor} in one of the next releases</em>
 * @author Bela Ban
 * @version $Id: Interceptor.java 2043 2006-06-06 10:20:05Z msurtani $
 */
public abstract class Interceptor implements InterceptorMBean {
   Interceptor next=null;
   TreeCache   cache=null;
   Log         log=null;
   boolean statsEnabled = true;      

   public Interceptor() {
      log=LogFactory.getLog(getClass());
   }


   public void setNext(Interceptor i) {
      next=i;
   }

   public Interceptor getNext() {
      return next;
   }

   public void setCache(TreeCache cache) {
      this.cache=cache;
   }

   public Object invoke(MethodCall m) throws Throwable {
      return next.invoke(m);
   }
   
   public boolean getStatisticsEnabled()
   {
      return statsEnabled;
   }
   
   public void setStatisticsEnabled(boolean enabled)
   {
      statsEnabled = enabled;
   }
   
   public Map dumpStatistics()
   {
      // should be implemented by individual interceptors
      return new HashMap();
   }
   
   public void resetStatistics()
   {
      // should be implemented by individual interceptors
   }

   /** Returns true if transaction is ACTIVE, false otherwise */
   protected boolean isActive(Transaction tx) {
      if(tx == null) return false;
      int status=-1;
      try {
         status=tx.getStatus();
         return status == Status.STATUS_ACTIVE;
      }
      catch(SystemException e) {
         log.error("failed getting transaction status", e);
         return false;
      }
   }

   /** Returns true if transaction is PREPARING, false otherwise */
   protected boolean isPreparing(Transaction tx) {
      if(tx == null) return false;
      int status=-1;
      try {
         status=tx.getStatus();
         return status == Status.STATUS_PREPARING;
      }
      catch(SystemException e) {
         log.error("failed getting transaction status", e);
         return false;
      }
   }

   /**
    * Return s true of tx's status is ACTIVE or PREPARING
    * @param tx
    * @return true if the tx is active or preparing
    */
   protected boolean isValid(Transaction tx) {
      return isActive(tx) || isPreparing(tx);
   }

    /**
     * Sets the invocation context
     * @param invocationContext
     */
    public void setInvocationContext(InvocationContext invocationContext)
    {
        cache.setInvocationContext( invocationContext );
    }

    /**
     * Retrieves an InvocationContext.
     * @return the context for the current invocation
     */
    public InvocationContext getInvocationContext()
    {
        return cache.getInvocationContext();
    }

    /**
     * This only works for prepare() and optimisticPrepare() method calls.
     * @param m
     */
    protected boolean isOnePhaseCommitPrepareMehod(JBCMethodCall m)
    {
       switch (m.getMethodId())
       {
          case MethodDeclarations.prepareMethod_id:
             return ((Boolean) m.getArgs()[3]).booleanValue();
          case MethodDeclarations.optimisticPrepareMethod_id:
             return ((Boolean) m.getArgs()[4]).booleanValue();
          default :
             return false;
       }
    }

    protected boolean isTransactionLifecycleMethod(JBCMethodCall mc)
    {
        int id = mc.getMethodId();
        return id == MethodDeclarations.commitMethod_id ||
                id == MethodDeclarations.rollbackMethod_id ||
                id == MethodDeclarations.prepareMethod_id ||
                id == MethodDeclarations.optimisticPrepareMethod_id;
    }

    protected boolean isBuddyGroupOrganisationMethod(JBCMethodCall mc)
    {
        int id = mc.getMethodId();

        return id == MethodDeclarations.remoteAnnounceBuddyPoolNameMethod_id ||
                id == MethodDeclarations.remoteAssignToBuddyGroupMethod_id ||
                id == MethodDeclarations.remoteRemoveFromBuddyGroupMethod_id;
    }
}
