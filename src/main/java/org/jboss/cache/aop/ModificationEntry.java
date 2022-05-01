/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop;

import org.jboss.cache.Fqn;
import org.jboss.aop.InstanceAdvisor;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Field;

/**
 * Modification entry for undo compensation. This is kind of ad hoc now.
 * We need to come up with an more formal undo operations to handle other cases
 * like object graph.
 * @author Ben Wang
 */
public class ModificationEntry
{
   public static final int INTERCEPTOR_ADD = 0;
   public static final int INTERCEPTOR_REMOVE = 1;
   public static final int COLLECTION_REPLACE = 2;
   private InstanceAdvisor advisor_;
   private BaseInterceptor interceptor_;
   private Field field_;
   private Object key_;
   private Object oldValue_;

   int opType_;

   public ModificationEntry(InstanceAdvisor advisor, BaseInterceptor interceptor, int op)
   {
      advisor_ = advisor;
      interceptor_ = interceptor;
      opType_ = op;
   }

   public ModificationEntry(Field field, Object key, Object oldValue)
   {
      field_ = field;
      key_ = key;
      oldValue_ = oldValue;
      opType_ = COLLECTION_REPLACE;
   }

   public int getOpType() { return opType_; }

   public InstanceAdvisor getInstanceAdvisor() { return advisor_; }

   public BaseInterceptor getCacheInterceptor() { return interceptor_; }


   public Field getField() {
      return field_;
   }

   public Object getKey() {
      return key_;
   }

   public Object getOldValue() {
      return oldValue_;
   }

}
