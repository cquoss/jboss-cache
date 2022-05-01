/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;


/**
 * Periodically (or when certain size is exceeded) takes elements and replicates them.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> May 24, 2003
 * @version $Revision: 3706 $
 */
public class ReplicationQueue {

   private static Log log=LogFactory.getLog(ReplicationQueue.class);

   private TreeCache cache=null;

   /** We flush every 5 seconds. Inactive if -1 or 0 */
   private long interval=5000;

   /** Max elements before we flush */
   private long max_elements=500;

   /** Holds the replication jobs: LinkedList<MethodCall> */
   private List elements=new ArrayList();

   /** For periodical replication */
   private Timer timer=null;

   /** The timer task, only calls flush() when executed by Timer */
   private MyTask task=null;

   public ReplicationQueue() {
   }

   /**
    * Constructs a new ReplicationQueue.
    */
   public ReplicationQueue(TreeCache cache, long interval, long max_elements) {
      this.cache=cache;
      this.interval=interval;
      this.max_elements=max_elements;
   }

   /**
    * Returns the flush interval in milliseconds.
    */
   public long getInterval() {
      return interval;
   }

   /**
    * Sets the flush interval in milliseconds.
    */
   public void setInterval(long interval) {
      this.interval=interval;
      stop();
      start();
   }

   /**
    * Returns the maximum number of elements to hold.
    * If the maximum number is reached, flushes in the calling thread.
    */
   public long getMax_elements() {
      return max_elements;
   }

   /**
    * Sets the maximum number of elements to hold.
    */
   public void setMax_elements(long max_elements) {
      this.max_elements=max_elements;
   }

   /**
    * Starts the asynchronous flush queue.
    */
   public synchronized void start() {
      if(interval > 0) {
         if(task == null)
            task=new MyTask();
         if(timer == null) {
            timer=new Timer(true);
            timer.schedule(task,
                    500, // delay before initial flush
                    interval); // interval between flushes
         }
      }
   }

   /**
    * Stops the asynchronous flush queue.
    */
   public synchronized void stop() {
      if(task != null) {
         task.cancel();
         task=null;
      }
      if(timer != null) {
         timer.cancel();
         timer=null;
      }
   }


   /**
    * Adds a new method call.
    */
   public void add(MethodCall job) {
      if (job == null)
         throw new NullPointerException("job is null");
      synchronized (elements) {
         elements.add(job);
         if (elements.size() >= max_elements)
            flush();
      }
   }

   /**
    * Flushes existing method calls.
    */
   public void flush() {
      List l;
      synchronized(elements) {
         if (log.isTraceEnabled())
            log.trace("flush(): flushing repl queue (num elements=" + elements.size() + ")");
         l = new ArrayList(elements);
         elements.clear();
      }

      if (l.size() > 0)
      {
         try {
            // send to all live nodes in the cluster
            cache.callRemoteMethods(null, MethodDeclarations.replicateAllMethod, new Object[]{l}, false, true, 5000);
         }
         catch(Throwable t) {
            log.error("failed replicating " + l.size() + " elements in replication queue", t);
         }
      }
   }

   class MyTask extends TimerTask {
      public void run() {
         flush();
      }
   }

}
