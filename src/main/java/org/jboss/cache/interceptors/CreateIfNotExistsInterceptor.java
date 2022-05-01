package org.jboss.cache.interceptors;

import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;
import org.jboss.cache.*;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jboss.cache.marshall.JBCMethodCall;
import org.jgroups.blocks.MethodCall;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Handles putXXX() methods: if the given node doesn't exist, it will be created
 * (depending on the create_if_not_exists argument)
 * @author Bela Ban
 * @version $Id: CreateIfNotExistsInterceptor.java 2059 2006-06-08 22:00:35Z msurtani $
 * @deprecated This code is not used anymore and will be removed in a future release
 */
public class CreateIfNotExistsInterceptor extends Interceptor {

   private final ReentrantLock put_lock=new ReentrantLock();

   private final ReentrantLock remove_lock=new ReentrantLock();

   static final List           putMethods=new ArrayList(4);

   /** FQNs which are the target of put(). Remove() methods need to block until those FQNs have been
    * created and/or updated */
   private final ArrayList     put_list=new ArrayList();

   private final ArrayList     remove_list=new ArrayList();

   static {
      putMethods.add(MethodDeclarations.putDataEraseMethodLocal);
      putMethods.add(MethodDeclarations.putDataMethodLocal);
      putMethods.add(MethodDeclarations.putKeyValMethodLocal);
      putMethods.add(MethodDeclarations.putFailFastKeyValueMethodLocal);
   }


   public void setCache(TreeCache cache) {
      super.setCache(cache);
   }

/*   public Object invoke(MethodCall m) throws Throwable {

      // we need to sync here - might have concurrent put() and remove() calls
      synchronized(this) { // only 1 thread can be here at any time - missing node needs to be created only once
         if(putMethods.contains(m.getMethod())) {
            Object[] args=m.getArgs();
            Fqn fqn=(Fqn)(args != null? args[1] : null);
            if(fqn == null)
               throw new CacheException("failed extracting FQN from method " + m);
            if(!cache.exists(fqn)) {
               GlobalTransaction gtx=cache.getCurrentTransaction();
               createNode(fqn, gtx);
               lock(fqn, DataNode.LOCK_TYPE_WRITE, false);
               // return super.invoke(m); // we need to execute the locking and put() in the same sync block !
            }
         }
      }
      return super.invoke(m);
   }*/



//   /**
//    * Synchronize between put(), remove() and evict() methods. This is coarse-grained, and should be replaced
//    * with FQN-based synchronization, e.g. put("/1/2/3" should <em>not</em> synchronize with remove("/a/b/c").
//    * @param m
//    * @return
//    * @throws Throwable
//    */
//   public Object invoke(MethodCall m) throws Throwable {
//      Method meth=m.getMethod();
//
//      boolean isPut=putMethods.contains(meth),
//            isRemove=TreeCache.removeNodeMethodLocal.equals(meth),
//            isEvict=TreeCache.evictNodeMethodLocal.equals(meth);
//
//      if(isPut || isRemove || isEvict) {  // we need to sync put(), remove() and evict() calls
//         try {
//            Object[] args=m.getArgs();
//            Fqn fqn=(Fqn)(args != null? (isEvict? args[0] : args[1]) : null);
//            if(fqn == null)
//               throw new CacheException("failed extracting FQN from method " + m);
//
//            lock.acquire();
//            if(isPut) { // lock needs to be held across puts()
//               if(!cache.exists(fqn)) {
//                  GlobalTransaction gtx=cache.getCurrentTransaction();
//                  if(log.isTraceEnabled())
//                     log.trace("creating node " + fqn);
//                  createNode(fqn, gtx);
//                  // lock(fqn, DataNode.LOCK_TYPE_WRITE, false);
//               }
//               else
//                  lock.release();
//            }
//            return super.invoke(m);
//         }
//         finally {
//            if(lock.holds() > 0)
//               lock.release();  // release lock for put()
//         }
//      }
//      return super.invoke(m); // no locks held for non put()/remove()/evict() methods (e.g. for get() methods)
//   }


   /**
    * Synchronize between put(), remove() and evict() methods. This is coarse-grained, and should be replaced
    * with FQN-based synchronization, e.g. put("/1/2/3" should <em>not</em> synchronize with remove("/a/b/c").
    * @param m
    * @return
    * @throws Throwable
    */
   public Object invoke(MethodCall call) throws Throwable {
      JBCMethodCall m = (JBCMethodCall) call;
      Method meth=m.getMethod();
      Fqn fqn;
      boolean isPut=putMethods.contains(meth),
            isRemove=m.getMethodId() == MethodDeclarations.removeNodeMethodLocal_id,
            isEvict=m.getMethodId() == MethodDeclarations.evictNodeMethodLocal_id;

      if(isPut || isRemove || isEvict) {  // we need to sync put(), remove() and evict() calls
         Object[] args=m.getArgs();
         fqn=(Fqn)(args != null? (isEvict? args[0] : args[1]) : null);
         if(fqn == null)
            throw new CacheException("failed extracting FQN from method " + m);

         if(isPut) { // lock needs to be held across puts()
            try {
               addFqnToPutList(fqn, put_lock);
               findAndBlockOnRemove(fqn, remove_lock);
               if(!cache.exists(fqn)) {
                  GlobalTransaction gtx=cache.getCurrentTransaction();
                  if(log.isTraceEnabled())
                     log.trace("creating node " + fqn);
                  createNode(fqn, gtx);
               }

               return super.invoke(m);
            }
            finally {
               removeFqnFromPutList(fqn, put_lock);
            }
         }
         else { // remove() or evict(): wait until all puts() that work on the same subtree have completed
            try {
               findAndBlockOnPut(fqn, put_lock);  // does NOT release put_lock !
               addFqnToRemoveList(fqn, remove_lock);
               put_lock.release();
               // we only release now because waiting on the put-list and adding to remove-list need to be atomic ! 
               return super.invoke(m);
            }
            finally {
               if(put_lock.holds() > 0)
                  put_lock.release();
               removeFqnFromRemoveList(fqn, remove_lock);
            }
         }
      }
      return super.invoke(m);
   }


   /**
    * Finds all FQNs in the put_list form which <code>fqn</code> is a parent (or equals), and waits on them.
    * Loops until no more matching FQNs are found or the list is empty.<p/>
    * <em>Don't</em> release the lock, the caller will release it !
    * @param fqn
    * @param lock
    * @throws InterruptedException
    */
   private void findAndBlockOnPut(Fqn fqn, ReentrantLock lock) throws InterruptedException {
      Fqn tmp;
      while(true) {
         //try {
            lock.acquire();
            tmp=findFqnInPutList(fqn);
            if(tmp == null) // put_list is empty, or fqn has not been found
               return;
            if(log.isTraceEnabled())
               log.trace("found " + tmp + " in put-list, waiting");
            synchronized(tmp) {
               lock.release();
               tmp.wait();
            }
            if(log.isTraceEnabled())
               log.trace("wait() for put-list on " + tmp + " got notified");
         //}
         //finally {
           // if(lock.holds() > 0)
             //  lock.release();
         //}
      }
   }

   /**
    * Finds all FQNs in the remove_list for which <code>fqn</code> is a child (or equals), and waits for them.
    * Loops until no more matching FQNs are found or the list is empty.
    * @param fqn
    * @param lock
    * @throws InterruptedException
    */
   private void findAndBlockOnRemove(Fqn fqn, ReentrantLock lock) throws InterruptedException {
      Fqn tmp;
      while(true) {
         lock.acquire();
         try {
            tmp=findFqnInRemoveList(fqn);
            if(tmp == null) // remove_list is empty, or fqn has not been found
               return;
            if(log.isTraceEnabled())
               log.trace("found " + tmp + " in remove-list, waiting");
            synchronized(tmp) {
               lock.release();
               tmp.wait();
            }
            if(log.isTraceEnabled())
               log.trace("wait() for remove-list on " + tmp + " got notified");
         }
         finally {
            lock.release();
         }
      }
   }


   private Fqn findFqnInPutList(Fqn fqn) {
      Fqn tmp;
      for(Iterator it=put_list.iterator(); it.hasNext();) {
         tmp=(Fqn)it.next();
         if(tmp.isChildOf(fqn) || tmp.equals(fqn))  // child or same, e.g. put(/a/b/c) and rem(/a/b) or rem(/a/b/c)
            return tmp;
      }
      return null;
   }

   private Fqn findFqnInRemoveList(Fqn fqn) {
      Fqn tmp;
      for(Iterator it=remove_list.iterator(); it.hasNext();) {
         tmp=(Fqn)it.next();
         if(fqn.isChildOf(tmp) || fqn.equals(tmp))  // child or same, e.g. put(/a/b/c) and rem(/a/b) or rem(/a/b/c)
            return tmp;
      }
      return null;
   }

   private void addFqnToPutList(Fqn fqn, ReentrantLock lock) 
      throws InterruptedException
   {
      lock.acquire();
      try {
         if(!put_list.contains(fqn)) {
            put_list.add(fqn);
            if(log.isTraceEnabled())
               log.trace("adding " + fqn + " to put-list (size=" + put_list.size() + ")");
         }
      }
      finally {
         lock.release();
      }
   }

   private void addFqnToRemoveList(Fqn fqn, ReentrantLock lock)
      throws InterruptedException
   {
      lock.acquire();
      try {
         if(!remove_list.contains(fqn)) {
            remove_list.add(fqn);
            if(log.isTraceEnabled())
               log.trace("adding " + fqn + " to remove-list (size=" + remove_list.size() + ")");
         }
      }
      finally {
         lock.release();
      }
   }



   private void removeFqnFromPutList(Fqn fqn, ReentrantLock lock)
      throws InterruptedException
   {
      lock.acquire();
      try {
         if(log.isTraceEnabled())
            log.trace("removing " + fqn + " from put-list (size=" + put_list.size() + ")");
         put_list.remove(fqn);
         lock.release();
         synchronized(fqn) {
            fqn.notifyAll();
         }
      }
      finally {
         lock.release();
      }
   }

   private void removeFqnFromRemoveList(Fqn fqn, ReentrantLock lock)
      throws InterruptedException
   {
      lock.acquire();
      try {
         if(log.isTraceEnabled())
            log.trace("removing " + fqn + " from remove-list (size=" + remove_list.size() + ")");
         remove_list.remove(fqn);
         lock.release();
         synchronized(fqn) {
            fqn.notifyAll();
         }
      }
      finally {
         lock.release();
      }
   }
    
   private void createNode(Fqn fqn, GlobalTransaction tx) {
      TreeNode n, child_node;
      Object child_name;
      Fqn tmp_fqn=Fqn.ROOT;

      if(fqn == null) return;
      synchronized(this) {
         int treeNodeSize=fqn.size();
         n=cache.getRoot();
         for(int i=0; i < treeNodeSize; i++) {
            child_name=fqn.get(i);
            tmp_fqn=new Fqn(tmp_fqn, child_name);
            child_node=n.getChild(child_name);
            if(child_node == null) {
               child_node=n.createChild(child_name, tmp_fqn, n);
               if(tx != null) {
                  MethodCall undo_op=MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal,
                                                    new Object[]{tx, tmp_fqn, Boolean.FALSE});
                  cache.addUndoOperation(tx, undo_op);

                  // add the node name to the list maintained for the current tx
                  // (needed for abort/rollback of transaction)
                  // cache.addNode(tx, (Fqn)tmp_fqn.clone());
               }
               cache.notifyNodeCreated(tmp_fqn);
            }
            n=child_node;
         }
      }
   }

}
