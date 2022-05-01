/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.interceptors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.eviction.EvictedEventNode;
import org.jboss.cache.eviction.Region;
import org.jboss.cache.eviction.RegionManager;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import java.util.HashMap;
import java.util.Map;

/**
 * Eviction Interceptor.
 * <p/>
 * This interceptor is used to handle eviction events.
 *
 * @author Daniel Huang
 * @version $Revision: 3477 $
 */
public class EvictionInterceptor extends Interceptor
{
   private static final Log log = LogFactory.getLog(EvictionInterceptor.class);

   protected RegionManager regionManager;
   protected Map evictionMethodHandlers = new HashMap();

   public EvictionInterceptor()
   {
      EvictionMethodHandler handler = new GetNodeEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.getNodeMethodLocal, handler);
      evictionMethodHandlers.put(MethodDeclarations.getDataMapMethodLocal, handler);

      handler = new GetKeyEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.getKeyValueMethodLocal, handler);

      handler = new RemoveNodeEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.removeNodeMethodLocal, handler);
      evictionMethodHandlers.put(MethodDeclarations.removeDataMethodLocal, handler);

      handler = new RemoveKeyEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.removeKeyMethodLocal, handler);

      handler = new PutDataEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.putDataMethodLocal, handler);

      handler = new PutDataEraseEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.putDataEraseMethodLocal, handler);

      handler = new PutKeyEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.putKeyValMethodLocal, handler);
      evictionMethodHandlers.put(MethodDeclarations.putFailFastKeyValueMethodLocal, handler);
      
      handler = new PartialEvictionEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.evictNodeMethodLocal, handler);
      evictionMethodHandlers.put(MethodDeclarations.evictVersionedNodeMethodLocal, handler);

   }

   /**
    * this method is for ease of unit testing. thus package access.
    * <p/>
    * Not to be attempted to be used anywhere else.
    */
   void setRegionManager(RegionManager regionManager)
   {
      this.regionManager = regionManager;
   }

   public void setCache(TreeCache cache)
   {
      super.setCache(cache);
      this.regionManager = cache.getEvictionRegionManager();
   }

   public Object invoke(MethodCall m) throws Throwable
   {
      Object ret = super.invoke(m);

      if (log.isTraceEnabled())
      {
         log.trace("Invoking EvictionInterceptor");
      }

      // skip the TX. this interceptor will invoke around/after the call and lock. if the ret == null or if an exception
      // is thrown, this interceptor is terminated. there is no need for explicit rollback logic.
      this.updateNode(m, ret);

      if (log.isTraceEnabled())
      {
         log.trace("Finished invoking EvictionInterceptor");
      }

      return ret;
   }

   protected void updateNode(MethodCall m, Object retVal)
   {
      if (log.isTraceEnabled())
      {
         log.trace("Updating node/element events with no tx");
      }

      EvictedEventNode event = this.extractEvent(m, retVal);
      if (event == null)
      {
         // no node modifications.
         if (log.isTraceEnabled()) log.trace("No node modifications"); 
         return;
      }

      this.doEventUpdatesOnRegionManager(event);

      if (log.isTraceEnabled())
      {
         log.trace("Finished updating node");
      }
   }

   protected EvictedEventNode extractEvent(MethodCall m, Object retVal)
   {
      EvictionMethodHandler handler = (EvictionMethodHandler) this.evictionMethodHandlers.get(m.getMethod());
      if (handler == null)
      {
         return null;
      }

      return handler.extractEvictedEventNode(m, retVal);
   }

   protected boolean canIgnoreEvent(Fqn fqn)
   {
      return regionManager.getRegion(fqn).getEvictionPolicy().canIgnoreEvent(fqn);
   }

   protected void doEventUpdatesOnRegionManager(EvictedEventNode event)
   {
      Region region = regionManager.getRegion(event.getFqn());
      region.putNodeEvent(event);

      if (log.isTraceEnabled())
      {
         log.trace("Adding event " + event.toString() + " to region at " + region.getFqn());
      }
   }

   protected class GetNodeEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         if (retVal == null)
         {
            if (log.isTraceEnabled())
            {
               log.trace("No event added. Node does not exist");
            }

            return null;
         }

         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[0];

         if (fqn != null && !EvictionInterceptor.this.canIgnoreEvent(fqn))
         {
            Region region = EvictionInterceptor.this.regionManager.getRegion(fqn);
            if (region.getEvictionPolicy().canIgnoreEvent(fqn))
            {
               return null;
            }
            return new EvictedEventNode(fqn, EvictedEventNode.VISIT_NODE_EVENT);
         }

         return null;
      }
   }

   protected class GetKeyEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         if (retVal == null)
         {
            if (log.isTraceEnabled())
            {
               log.trace("No event added. Element does not exist");
            }

            return null;
         }

         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[0];
         Object key = args[1];
         if (fqn != null && key != null && !EvictionInterceptor.this.canIgnoreEvent(fqn))
         {
            return new EvictedEventNode(fqn, EvictedEventNode.VISIT_NODE_EVENT);
         }

         return null;
      }

   }

   protected class RemoveNodeEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[1];

         if (fqn != null && !EvictionInterceptor.this.canIgnoreEvent(fqn))
         {
            return new EvictedEventNode(fqn, EvictedEventNode.REMOVE_NODE_EVENT);
         }

         return null;
      }

   }

   protected class RemoveKeyEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         if (retVal == null)
         {
            if (log.isTraceEnabled())
            {
               log.trace("No event added. Element does not exist");
            }

            return null;
         }

         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         Object key = args[2];
         if (fqn != null && key != null && !EvictionInterceptor.this.canIgnoreEvent(fqn))
         {
            return new EvictedEventNode(fqn, EvictedEventNode.REMOVE_ELEMENT_EVENT, 1);
         }
         return null;
      }

   }

   protected class PutDataEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object[] args = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         Map putData = (Map) args[2];
         if (fqn != null && !EvictionInterceptor.this.canIgnoreEvent(fqn))
         {
            if (putData == null)
            {
               if (log.isTraceEnabled())
               {
                  log.trace("Putting null data under fqn " + fqn + ".");
               }

               return null;
            }

            int size;
            synchronized (putData)
            {
               size = putData.size();
            }

            return new EvictedEventNode(fqn, EvictedEventNode.ADD_NODE_EVENT, size);
         }

         return null;
      }

   }

   protected class PutDataEraseEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object[] args = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         Map putData = (Map) args[2];
         Boolean resetElementCount = (Boolean) args[4];
         if (fqn != null && !EvictionInterceptor.this.canIgnoreEvent(fqn))
         {
            if (putData == null)
            {
               if (log.isTraceEnabled())
               {
                  log.trace("Putting null data under fqn " + fqn + ".");
               }

               return null;
            }

            int size;
            synchronized (putData)
            {
               size = putData.size();
            }

            EvictedEventNode event = new EvictedEventNode(fqn, EvictedEventNode.ADD_NODE_EVENT, size);
            event.setResetElementCount(resetElementCount.booleanValue());
            return event;
         }

         return null;
      }
   }

   protected class PutKeyEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object[] args = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         Object key = args[2];
         if (fqn != null && key != null && !EvictionInterceptor.this.canIgnoreEvent(fqn))
         {
            return new EvictedEventNode(fqn, EvictedEventNode.ADD_ELEMENT_EVENT, 1);
         }

         return null;
      }

   }
   
   protected class PartialEvictionEvictionMethodHandler implements EvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         // See if the node still exists; i.e. was only data removed
         // because it still has children.
         // If yes, put an ADD event in the queue so the node gets revisited
         
         boolean complete = (retVal != null && ((Boolean) retVal).booleanValue());
         if (!complete)
         {
            Object[] args = mc.getArgs();
            Fqn fqn = (Fqn) args[0];         
            if (fqn != null
                 && !EvictionInterceptor.this.canIgnoreEvent(fqn))
            {
               return new EvictedEventNode(fqn, EvictedEventNode.ADD_NODE_EVENT, 0);
            }
         }
         return null;
      }      
   }

   protected interface EvictionMethodHandler
   {
      EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal);
   }

}
