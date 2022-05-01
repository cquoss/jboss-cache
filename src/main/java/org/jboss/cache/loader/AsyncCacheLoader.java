/**
 * 
 */
package org.jboss.cache.loader;

import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedBoolean;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.TreeCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The AsyncCacheLoader is a delegating cache loader that passes on all
 * operations to an underlying CacheLoader.
 * 
 * Read operations are done synchronously, while write (CRUD - Create, Remove,
 * Update, Delete) operations are done asynchronously.  There is no provision
 * for exception handling at the moment for problems encountered with the
 * underlying CacheLoader during a CRUD operation, and the exception is just
 * logged.
 * 
 * When configuring the CacheLoader, use the following attribute:
 * 
 * <code>
 *       &lt;attribute name="CacheLoaderAsynchronous"&gt;true&lt;/attribute&gt;
 * </code>      
 * 
 * to define whether cache loader operations are to be asynchronous.  If not
 * specified, a cache loader operation is assumed synchronous.
 * <p>
 *
 * The following additional parameters are available: 
 <dl>
 <dt>cache.async.batchSize</dt>
    <dd>Number of modifications to commit in one transaction, default is
    100. The minimum batch size is 1.</dd>
 <dt>cache.async.pollWait</dt>
    <dd>How long to wait before processing an incomplete batch, in
    milliseconds.  Default is 100.  Set this to 0 to not wait before processing
    available records.</dd>
 <dt>cache.async.returnOld</dt>
    <dd>If <code>true</code>, this loader returns the old values from {@link
#put} and {@link #remove} methods.  Otherwise, these methods always return
null.  Default is true.  <code>false</code> improves the performance of these
operations.</dd>
 <dt>cache.async.queueSize</dt>  
    <dd>Maximum number of entries to enqueue for asynchronous processing.
    Lowering this size may help prevent out-of-memory conditions.  It also may
    help to prevent less records lost in the case of JVM failure.  Default is
    10,000 operations.</dd>
 <dt>cache.async.put</dt>  
    <dd>If set to false, all {@link #put} operations will be processed
    synchronously, and then only the {@link #remove} operations will be
    processed asynchronously. This mode may be useful for processing
    expiration of messages within a separate thread and keeping other
    operations synchronous for reliability.
    </dd>
 </dl>
 * For increased performance for many smaller transactions, use higher values
 * for <code>cache.async.batchSize</code> and
 * <code>cache.async.pollWait</code>.  For larger sized records, use a smaller
 * value for <code>cache.async.queueSize</code>.
 * 
 * @author Manik Surtani (manik.surtani@jboss.com)
 */
public class AsyncCacheLoader implements CacheLoader
{

   private static final Log log = LogFactory.getLog(AsyncCacheLoader.class); 

   private static SynchronizedInt threadId = new SynchronizedInt(0);

   /**
    * Default limit on entries to process asynchronously.
    */
   public static final int DEFAULT_QUEUE_SIZE = 10000;

   private CacheLoader delegateTo;
   private AsyncProcessor processor;
   private SynchronizedBoolean stopped = new SynchronizedBoolean(true);
   private BoundedLinkedQueue queue = new BoundedLinkedQueue(DEFAULT_QUEUE_SIZE);

   // Configuration keys

   private int batchSize = 100;
   private long pollWait = 100; // milliseconds
   private boolean returnOld = true;
   private boolean asyncPut = true;

   public AsyncCacheLoader()
   {
   }

   public AsyncCacheLoader(CacheLoader cacheLoader)
   {
      delegateTo = cacheLoader;
   }

   /**
    * Returns the delegate cache loader.
    */
   public CacheLoader getCacheLoader()
   {
       return delegateTo;
   }

   public void setConfig(Properties props)
   {
      log.debug("setConfig " + props);
      String s;
     
      s = props.getProperty("cache.async.batchSize");
      if (s != null)
         batchSize = Integer.parseInt(s);
      if (batchSize <= 0)
         throw new IllegalArgumentException("Invalid size: " + batchSize);

      s = props.getProperty("cache.async.pollWait");
      if (s != null)
         pollWait = Integer.parseInt(s);

      s = props.getProperty("cache.async.returnOld");
      if (s != null)
         returnOld = Boolean.valueOf(s).booleanValue();

      s = props.getProperty("cache.async.queueSize");
      if (s != null)
         queue = new BoundedLinkedQueue(Integer.parseInt(s));

      s = props.getProperty("cache.async.put");
      if (s != null)
         asyncPut = Boolean.valueOf(s).booleanValue();

      delegateTo.setConfig(props);
   }

   public void setCache(TreeCache c)
   {
      delegateTo.setCache( c );
   }

   public Set getChildrenNames(Fqn fqn) throws Exception
   {
      return delegateTo.getChildrenNames( fqn );
   }

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

//   public Object get(Fqn name, Object key) throws Exception
//   {
//      return delegateTo.get( name, key );
//   }

   public Map get(Fqn name) throws Exception
   {
      try 
      {
         return delegateTo.get( name );
      }
      catch (IOException e)
      {
         // FileCacheLoader sometimes does this apparently
         log.trace(e);
         return new HashMap(); // ?
      }
   }

   public boolean exists(Fqn name) throws Exception
   {
      return delegateTo.exists( name );
   }

   Object get(Fqn name, Object key) throws Exception
   {
      if (returnOld)
      {
         try
         {
             Map map = delegateTo.get( name );
             if (map != null)
                return map.get( key );
         }
         catch (IOException e)
         {
            // FileCacheLoader sometimes does this apparently
            log.trace(e);
         }
      }
      return null;
   }

   public Object put(Fqn name, Object key, Object value) throws Exception
   {
      if (asyncPut)
      {
         Object oldValue = get(name, key);
         Modification mod = new Modification(Modification.PUT_KEY_VALUE, name, key, value);
         enqueue(mod);
         return oldValue;
      } else return delegateTo.put(name, key, value);
   }

   public void put(Fqn name, Map attributes) throws Exception
   {
      if (asyncPut)
      {
         // JBCACHE-769 -- make a defensive copy
         Map attrs = (attributes == null ? null : new HashMap(attributes));
         Modification mod = new Modification(Modification.PUT_DATA, name, attrs);
         enqueue(mod);
      } 
      else delegateTo.put(name, attributes); // Let delegate make its own defensive copy
   }

   public void put(List modifications) throws Exception
   {
      if (asyncPut)
      {
         Iterator i = modifications.iterator();
         while (i.hasNext())
            enqueue((Modification)i.next());
      } 
      else delegateTo.put(modifications);
   }

   public Object remove(Fqn name, Object key) throws Exception
   {
      Object oldValue = get(name, key);
      Modification mod = new Modification(Modification.REMOVE_KEY_VALUE, name, key);
      enqueue(mod);
      return oldValue;
   }

   public void remove(Fqn name) throws Exception
   {
      Modification mod = new Modification(Modification.REMOVE_NODE, name);
      enqueue(mod);
   }

   public void removeData(Fqn name) throws Exception
   {
      Modification mod = new Modification(Modification.REMOVE_DATA, name);
      enqueue(mod);
   }

   public void prepare(Object tx, List modifications, boolean one_phase)
         throws Exception
   {
      delegateTo.prepare( tx, modifications, one_phase );
   }

   public void commit(Object tx) throws Exception
   {
      delegateTo.commit( tx );
   }

   public void rollback(Object tx)
   {
      delegateTo.rollback( tx );
   }

   public byte[] loadEntireState() throws Exception
   {
      return delegateTo.loadEntireState();
   }

   void storeState(byte[] state, Fqn subtree) throws Exception
   {
      Modification mod = new Modification(Modification.STORE_STATE, subtree, null, state);
      enqueue(mod);
   }

   /**
    * Stores the entire state.
    * This is processed asynchronously.
    */
   public void storeEntireState(byte[] state) throws Exception
   {
      Modification mod = new Modification(Modification.STORE_STATE, null, null, state);
      enqueue(mod);
   }

   public void create() throws Exception
   {
      delegateTo.create();
   }

   public void start() throws Exception
   {
      if (log.isInfoEnabled()) log.info("Async cache loader starting: " + this);
      stopped.set(false);
      delegateTo.start();
      processor = new AsyncProcessor();
      processor.start();
   }

   public void stop()
   {
      stopped.set(true);
      if (processor != null)
         processor.stop();
      delegateTo.stop();
   }

   public void destroy()
   {
      delegateTo.destroy();
   }

   private void enqueue(Modification mod)
      throws CacheException, InterruptedException
   {
      if (stopped.get())
         throw new CacheException("AsyncCacheLoader stopped; no longer accepting more entries.");
      queue.put(mod);
   }

   /**
    * Processes (by batch if possible) a queue of {@link Modification}s.
    * @author manik surtani
    */
   private class AsyncProcessor implements Runnable
   {
      private Thread t;

      // Modifications to process as a single put
      private final List mods = new ArrayList(batchSize);

      public void start() {
         if (t == null || !t.isAlive())
         {
             t = new Thread(this, "AsyncCacheLoader-" + threadId.increment());
             //t.setDaemon(true);
             t.start();
         }
      }

      public void stop() {
         if (t != null)
         {
            t.interrupt();
            try
            {
               t.join();
            } catch (InterruptedException e) {}
         }
         if (!queue.isEmpty())
            log.warn("Async queue not yet empty, possibly interrupted");
      }

      public void run()
      {
         while (!Thread.interrupted()) 
         {
            try
            {
               run0();
            }
            catch (InterruptedException e)
            {
               break;
            }
         }

         try
         {
            if (log.isTraceEnabled()) log.trace("process remaining batch " + mods.size());
            put(mods);
            if (log.isTraceEnabled()) log.trace("process remaining queued " + queue.size());
            while (!queue.isEmpty())
               run0();
         }
         catch (InterruptedException e)
         {
            log.trace("remaining interrupted");
         }
      }

      private void run0() throws InterruptedException {
         log.trace("run0");
         Object o = queue.take();
         addTaken(o);
         while (mods.size() < batchSize)
         {
            o = queue.poll(pollWait);
            if (o == null)
               break;
            addTaken(o);
         }
         if (log.isTraceEnabled())
            log.trace("put " + mods.size());
         put(mods);
         mods.clear();
      }

      private void addTaken(Object o)
         throws InterruptedException
      {
         if (o instanceof List)
         {
            mods.addAll((List)o);
         }
         else
         {
            Modification mod = (Modification)o;
            if (mod.getType() == Modification.STORE_STATE)
            {
               log.trace("storeState");
               storeState(mod.getFqn(), (byte [])mod.getValue());
            }
            else
            {
               mods.add(mod);
            }
         }
      }

      private void storeState(Fqn fqn, byte b[]) {
         try
         {
            if (fqn == null)
               delegateTo.storeEntireState(b);
            else
               ((ExtendedCacheLoader)delegateTo).storeState(b, fqn);
         }
         catch (Exception e)
         {
            if (log.isWarnEnabled()) log.warn("Failed to store " + e);
            log.debug("Exception: ", e);
         }
      }

      private void put(List mods) {
         try
         {
            delegateTo.put(mods);
         }
         catch (Exception e)
         {
            if (log.isWarnEnabled()) log.warn("Failed to process async modifications: " + e);
            log.debug("Exception: ", e);
         }
      }

      public String toString() {
         return "TQ t=" + t;
      }

   }

   public String toString() {
      return super.toString() + 
         " delegate=[" + delegateTo + "]" +
         " processor=" + processor +
         " stopped=" + stopped +
         " batchSize=" + batchSize +
         " pollWait=" + pollWait +
         " returnOld=" + returnOld +
         " asyncPut=" + asyncPut +
         " queue.capacity()=" + queue.capacity() +
         " queue.peek()=" + queue.peek();
   }

}
