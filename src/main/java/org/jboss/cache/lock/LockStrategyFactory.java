/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.lock;


/**
 * Factory to create LockStragtegy instance.
 *
 * @author Ben Wang
 */
public class LockStrategyFactory
{

   /**
    * Transaction locking isolation level. Default.
    */
   private static IsolationLevel lockingLevel_ = IsolationLevel.REPEATABLE_READ;

   /**
    *
    */
   protected LockStrategyFactory()
   {
   }

   public static LockStrategy getLockStrategy() {
      return getLockStrategy(lockingLevel_);
   }

   public static LockStrategy getLockStrategy(IsolationLevel lockingLevel)
   {
      //if(log_.isTraceEnabled()) {
        // log_.trace("LockStrategy is: " + lockingLevel);
      //}
      if (lockingLevel == null || lockingLevel == IsolationLevel.NONE)
         return new LockStrategyNone();
      if (lockingLevel == IsolationLevel.REPEATABLE_READ)
         return new LockStrategyRepeatableRead();
      if (lockingLevel == IsolationLevel.SERIALIZABLE)
         return new LockStrategySerializable();
      if (lockingLevel == IsolationLevel.READ_COMMITTED)
         return new LockStrategyReadCommitted();
      if (lockingLevel == IsolationLevel.READ_UNCOMMITTED)
         return new LockStrategyReadUncommitted();
      throw new RuntimeException("getLockStrategy: LockStrategy selection not recognized." +
            " selection: " + lockingLevel);
   }

   public static void setIsolationLevel(IsolationLevel level)
   {
      lockingLevel_ = level;
   }

}
