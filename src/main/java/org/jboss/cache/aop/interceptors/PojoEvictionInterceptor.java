package org.jboss.cache.aop.interceptors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.DataNode;
import org.jboss.cache.aop.AOPInstance;
import org.jboss.cache.aop.InternalDelegate;
import org.jboss.cache.eviction.EvictedEventNode;
import org.jboss.cache.eviction.Region;
import org.jboss.cache.interceptors.EvictionInterceptor;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import java.util.Iterator;
import java.util.Set;

/**
 * AOP specific eviction interceptor implementation.
 *
 * @author Daniel Huang (dhuang@jboss.org)
 */
public class PojoEvictionInterceptor extends EvictionInterceptor
{
   private static final Log log_ = LogFactory.getLog(PojoEvictionInterceptor.class);

   private TreeCache cache = null;

   public PojoEvictionInterceptor()
   {
      super();

      // now override the handler lookup map with AOP specific handlers.
      EvictionMethodHandler handler = new PojoGetNodeEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.getNodeMethodLocal, handler);
      evictionMethodHandlers.put(MethodDeclarations.getDataMapMethodLocal, handler);

      handler = new PojoGetKeyEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.getKeyValueMethodLocal, handler);

      handler = new PojoRemoveNodeEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.removeNodeMethodLocal, handler);
      evictionMethodHandlers.put(MethodDeclarations.removeDataMethodLocal, handler);

      handler = new PojoRemoveKeyEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.removeKeyMethodLocal, handler);

      handler = new PojoPutDataEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.putDataMethodLocal, handler);

      handler = new PojoPutDataEraseEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.putDataEraseMethodLocal, handler);

      handler = new PojoPutKeyEvictionMethodHandler();
      evictionMethodHandlers.put(MethodDeclarations.putKeyValMethodLocal, handler);
      evictionMethodHandlers.put(MethodDeclarations.putFailFastKeyValueMethodLocal, handler);

   }

   public void setCache(TreeCache cache)
   {
      super.setCache(cache);
      this.cache = cache;
   }

   boolean isAopNode(Fqn fqn)
   {
      // Use this API so it doesn't go thru the interceptor.
      DataNode node = null;
      node = cache.peek(fqn);

      if( node.get(AOPInstance.KEY) != null )
         return true;
      else
         return false;
   }

   private void visitChildrenRecursively(Region region, Fqn fqn)
   {
      Set set = null;
      try
      {
         set = cache.getChildrenNames(fqn);
      }
      catch (CacheException e)
      {
         e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      int size = (set == null) ? 0 : set.size();
      if (log_.isTraceEnabled())
      {
         log_.trace("nodeVisited(): is an aop node. fqn- " + fqn + " size of children is " + size);
      }

      if (set == null) return; // barren
      Iterator it = set.iterator();
      while (it.hasNext())
      {
         Object childName = it.next();
         Fqn fqnKid = new Fqn(fqn, childName);
         visitChildrenRecursively(region, fqnKid);
         region.setVisitedNode(fqnKid);
      }
   }

   private boolean isInternalNode(Fqn fqn)
   {
      return InternalDelegate.isInternalNode(fqn);
   }

   protected class PojoGetNodeEvictionMethodHandler extends GetNodeEvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         EvictedEventNode event = super.extractEvictedEventNode(mc, retVal);

         if (event != null)
         {
            Object args[] = mc.getArgs();
            Fqn fqn = (Fqn) args[0];

            Region region = regionManager.getRegion(fqn);
            if (isAopNode(fqn))
            {
               visitChildrenRecursively(region, fqn);
            }

            if (fqn != null && !isInternalNode(fqn))
            {
               return new EvictedEventNode(fqn, EvictedEventNode.VISIT_NODE_EVENT);
            }
         }

         return null;
      }
   }

   protected class PojoGetKeyEvictionMethodHandler extends GetKeyEvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         EvictedEventNode event = super.extractEvictedEventNode(mc, retVal);

         if (event != null)
         {
            Object args[] = mc.getArgs();
            Fqn fqn = (Fqn) args[0];

            Region region = regionManager.getRegion(fqn);
            if (isAopNode(fqn))
            {
               visitChildrenRecursively(region, fqn);
            }

            if (fqn != null && !isInternalNode(fqn))
            {
               return new EvictedEventNode(fqn, EvictedEventNode.VISIT_NODE_EVENT);
            }
         }

         return null;
      }
   }

   protected class PojoRemoveNodeEvictionMethodHandler extends RemoveNodeEvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         if (isInternalNode(fqn)) return null;

         return super.extractEvictedEventNode(mc, retVal);
      }
   }

   protected class PojoRemoveKeyEvictionMethodHandler extends RemoveKeyEvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         if (isInternalNode(fqn)) return null;

         return super.extractEvictedEventNode(mc, retVal);
      }
   }

   protected class PojoPutDataEvictionMethodHandler extends PutDataEvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         if (isInternalNode(fqn)) return null;

         return super.extractEvictedEventNode(mc, retVal);
      }
   }

   protected class PojoPutDataEraseEvictionMethodHandler extends PutDataEraseEvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         if (isInternalNode(fqn)) return null;

         return super.extractEvictedEventNode(mc, retVal);
      }
   }

   protected class PojoPutKeyEvictionMethodHandler extends PutKeyEvictionMethodHandler
   {
      public EvictedEventNode extractEvictedEventNode(MethodCall mc, Object retVal)
      {
         Object args[] = mc.getArgs();
         Fqn fqn = (Fqn) args[1];
         if (isInternalNode(fqn)) return null;

         return super.extractEvictedEventNode(mc, retVal);
      }
   }

}
