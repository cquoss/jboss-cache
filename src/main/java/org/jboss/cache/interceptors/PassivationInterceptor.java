package org.jboss.cache.interceptors;

import org.jboss.cache.Fqn;
import org.jboss.cache.TreeNode;
import org.jboss.cache.TreeCache;
import org.jboss.cache.loader.CacheLoader;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jgroups.blocks.MethodCall;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;

import java.util.HashMap;
import java.util.Map;

/**
 * Writes evicted nodes back to the store on the way in through the
 * CacheLoader, either before each method call (no TXs), or at TX commit.
 * 
 * @author <a href="mailto:{hmesha@novell.com}">{Hany Mesha}</a>
 * @version $Id: PassivationInterceptor.java 2758 2006-10-30 04:40:57Z bstansberry $
 */
public class PassivationInterceptor extends Interceptor implements PassivationInterceptorMBean {
   
   protected CacheLoader loader = null;
   private SynchronizedLong m_passivations = new SynchronizedLong(0);

   public void setCache(TreeCache cache) {
      super.setCache(cache);
      this.loader = cache.getCacheLoader();
   }

   /**
    * Notifies the cache instance listeners that the evicted node is about to
    * be passivated and stores the evicted node and its attributes back to the
    * store using the CacheLoader.
    *
    * @param m
    * @return
    * @throws Throwable
    */
   public Object invoke(MethodCall call) throws Throwable {
      
      JBCMethodCall m = (JBCMethodCall) call;
      
      // hmesha- We don't need to handle transaction during passivation since
      // passivation happens local to a node and never replicated

      // evict() method need to be applied to the CacheLoader before passing up the call
      if (m.getMethodId() == MethodDeclarations.evictNodeMethodLocal_id) {
         Object[]     args=m.getArgs();
         Fqn fqn = (Fqn)args[0];
         try
         {
            synchronized (this) {
               // evict method local doesn't hold attributes 
               // therefore we have to get them manually
               Map attributes = getNodeAttributes(fqn);
               
               // remove internal flag if node was never fully loaded
               if (attributes != null)
                  attributes.remove(TreeCache.UNINITIALIZED);
               
               // notify listeners that this node is about to be passivated
               cache.notifyNodePassivate(fqn, true);
               loader.put(fqn, attributes);
            }
            if (statsEnabled && cache.getUseInterceptorMbeans())
               m_passivations.increment();
         }
         catch (NodeNotLoadedException e)
         {
            if (log.isTraceEnabled())
            {
               log.trace("Node " + fqn + " not loaded in memory; passivation skipped");
            }
            // TODO should we just return here and abort the rest of the 
            // interceptor chain? Probably a bad idea.
         }
         
      }

      return super.invoke(m);
   }
   
   public long getPassivations() {
      return m_passivations.get();  
   }
   
   public void resetStatistics() {
      m_passivations.set(0);
   }
   
   public Map dumpStatistics() {
      Map retval=new HashMap();
      retval.put("Passivations", new Long(m_passivations.get()));
      return retval;
   }
   
   /**
    * Returns attributes for a node.
    */
   private Map getNodeAttributes(Fqn fqn) throws NodeNotLoadedException 
   {
      if (fqn == null)
         throw new NodeNotLoadedException();
      
      TreeNode n = cache.getRoot();
      int size = fqn.size();
      for(int i=0; i < size && n != null; i++) {
         n = n.getChild(fqn.get(i));
      }
      if (n != null)
         return n.getData();
      else
         throw new NodeNotLoadedException();
   }
   
   private static class NodeNotLoadedException extends Exception
   {
      /** The serialVersionUID */
      private static final long serialVersionUID = -4078972305344328905L;      
   }
}
