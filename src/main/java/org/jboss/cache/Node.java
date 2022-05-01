/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.factories.NodeFactory;
import org.jboss.cache.lock.IdentityLock;
import org.jboss.cache.lock.LockingException;
import org.jboss.cache.lock.TimeoutException;
import org.jboss.cache.lock.UpgradeException;
import org.jboss.cache.marshall.MethodCallFactory;
import org.jboss.cache.marshall.MethodDeclarations;
import org.jgroups.blocks.MethodCall;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Basic data node class.
 */
public class Node extends AbstractNode implements Externalizable
{

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -5040432493172658796L;

   private static Log log = LogFactory.getLog(Node.class);

   /**
    * Cached for performance.
    */
   private static boolean trace = log.isTraceEnabled();

   /**
    * True if all children have been loaded.
    * This is set when TreeCache.getChildrenNames() is called.
    */
   private boolean children_loaded = false;

   /**
    * Lock manager that manages locks to be acquired when accessing the node
    * inside a transaction. Lazy set just in case locking is not needed.
    */
   private IdentityLock lock_ = null;

   /**
    * A reference of the TreeCache instance.
    */
   private TreeCache cache;

   /**
    * Construct an empty node; used by serialization.
    */
   public Node()
   {
   }

   /**
    * Constructs a new node with a name, etc.
    */
   public Node(Object child_name, Fqn fqn, Node parent, Map data, TreeCache cache)
   {
      init(child_name, fqn, cache);
      if (data != null)
      {
         this.data().putAll(data);
      }
   }

   /**
    * Constructs a new node with a name, etc.
    *
    * @param mapSafe <code>true</code> if param <code>data</code> can safely be
    *                directly assigned to this object's {@link #data} field;
    *                <code>false</code> if param <code>data</code>'s contents
    *                should be copied into this object's {@link #data} field.
    */
   public Node(Object child_name, Fqn fqn, Node parent, Map data, boolean mapSafe,
               TreeCache cache)
   {
      init(child_name, fqn, cache);
      if (data != null)
      {
         if (mapSafe)
            this.data = data;
         else
            this.data().putAll(data);
      }
   }

   /**
    * Constructs a new node with a single key and value.
    */
   public Node(Object child_name, Fqn fqn, Node parent, Object key, Object value, TreeCache cache)
   {
      init(child_name, fqn, cache);
      data().put(key, value);
   }

   /**
    * Initializes with a name and FQN and cache.
    */
   protected final void init(Object child_name, Fqn fqn, TreeCache cache)
   {
      if (cache == null)
         throw new IllegalArgumentException("no cache init for " + fqn);
      this.cache = cache;
      this.fqn = fqn;
      if (!fqn.isRoot() && !child_name.equals(fqn.getLast()))
         throw new IllegalArgumentException("Child " + child_name + " must be last part of " + fqn);
   }

   /**
    * Returns a parent by checking the TreeMap by name.
    */
   public TreeNode getParent()
   {
      if (fqn.isRoot())
         return null;
      return cache.peek(fqn.getParent());
   }

   private synchronized void initLock()
   {
      if (lock_ == null)
         lock_ = new IdentityLock(cache, fqn);
   }

   protected synchronized Map children()
   {
      if (children == null)
      {
         if (getFqn().isRoot())
         {
            children = new ConcurrentReaderHashMap(64);
         }
         else
         {
            children = new ConcurrentReaderHashMap(4);
         }
      }
      return children;
   }

   private void setTreeCacheInstance(TreeCache cache)
   {
      this.cache = cache;
      this.lock_ = null;
   }

   public Map getChildren(boolean includeMarkedForRemoval)
   {
      return includeMarkedForRemoval ? children : getChildren();
   }

   public Map getChildren()
   {
      if (children == null) return null;
      if (children.isEmpty()) return children;

      // make sure we remove all children that are marked for removal ...
      Map toReturn = new HashMap();
      Iterator c = children.values().iterator();
      while (c.hasNext())
      {
         Node ch = (Node) c.next();
         if (!ch.isMarkedForRemoval())
         {
            toReturn.put(ch.getName(), ch);
         }
      }

      return toReturn;
   }

   /**
    * Set the tree cache instance recursively down to the children as well.
    * Note that this method is not currently thread safe.
    */
   public void setRecursiveTreeCacheInstance(TreeCache cache)
   {

      setTreeCacheInstance(cache);

      if (children != null)
      {
         for (Iterator it = children.keySet().iterator(); it.hasNext();)
         {
            DataNode nd = (DataNode) children.get(it.next());
            nd.setRecursiveTreeCacheInstance(cache);
         }
      }
   }

   public boolean getChildrenLoaded()
   {
      return children_loaded;
   }

   public void setChildrenLoaded(boolean flag)
   {
      children_loaded = flag;
   }

   public Object get(Object key)
   {
      synchronized (this)
      {
         return data == null ? null : data.get(key);
      }
   }

   public boolean containsKey(Object key)
   {
      synchronized (this)
      {
         return data != null && data.containsKey(key);
      }
   }

   /**
    * Returns the data keys, or an empty set if there are no keys.
    */
   public Set getDataKeys()
   {
      synchronized (this)
      {
         if (data == null)
            //return Collections.emptySet(); // this is JDK5 only!!  Sucks!
            return new HashSet(0);
         return data.keySet();
      }
   }

   boolean isReadLocked()
   {
      return lock_ != null && lock_.isReadLocked();
   }

   boolean isWriteLocked()
   {
      return lock_ != null && lock_.isWriteLocked();
   }

   public boolean isLocked()
   {
      return isWriteLocked() || isReadLocked();
   }

   /**
    * @deprecated Use getLock() instead
    */
   public IdentityLock getImmutableLock()
   {
      initLock();
      return lock_;
   }

   public IdentityLock getLock()
   {
      initLock();
      return lock_;
   }

   public Map getData()
   {
      synchronized (this)
      {
         if (data == null)
            return null;
         return new HashMap(data);
      }
   }

   public int numAttributes()
   {
      synchronized (this)
      {
         return data != null ? data.size() : 0;
      }
   }

   public void put(Map data, boolean erase)
   {
      synchronized (this)
      {
         if (erase)
         {
            if (this.data != null)
               this.data.clear();
         }
         if (data == null) return;
         this.data().putAll(data);
      }
   }

   public Object put(Object key, Object value)
   {
      synchronized (this)
      {
         return this.data().put(key, value);
      }
   }

   public TreeNode getOrCreateChild(Object child_name, GlobalTransaction gtx, boolean createIfNotExists)
   {
      DataNode child;
      if (child_name == null)
         throw new IllegalArgumentException("null child name");

      child = (DataNode) children().get(child_name);
      if (createIfNotExists && child == null)
      {
         // construct the new child outside the synchronized block to avoid
         // spending any more time than necessary in the synchronized section
         Fqn child_fqn = new Fqn(this.fqn, child_name);
         DataNode newChild = (DataNode) NodeFactory.getInstance().createNodeOfType(this, child_name, child_fqn, this, null, cache);
         if (newChild == null)
            throw new IllegalStateException();
         synchronized (this)
         {
            // check again to see if the child exists
            // after acquiring exclusive lock
            child = (Node) children().get(child_name);
            if (child == null)
            {
               child = newChild;
               children.put(child_name, child);
               if (gtx != null)
               {
                  MethodCall undo_op = MethodCallFactory.create(MethodDeclarations.removeNodeMethodLocal,
                          new Object[]{gtx, child_fqn, Boolean.FALSE});
                  cache.addUndoOperation(gtx, undo_op);
                  // add the node name to the list maintained for the current tx
                  // (needed for abort/rollback of transaction)
                  // cache.addNode(gtx, child.getFqn());
               }
            }
         }

         // notify if we actually created a new child
         if (newChild == child)
         {
            if (trace)
            {
               log.trace("created child: fqn=" + child_fqn);
            }
            cache.notifyNodeCreated(child.getFqn());
         }
      }
      return child;

   }

   public TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent)
   {
      return getOrCreateChild(child_name, null, true);
   }

   public TreeNode createChild(Object child_name, Fqn fqn, TreeNode parent, Object key, Object value)
   {
      TreeNode n = getOrCreateChild(child_name, null, true);
      n.put(key, value);
      return n;
   }

   public Object remove(Object key)
   {
      synchronized (this)
      {
         return data != null ? data.remove(key) : null;
      }
   }

   public void clear()
   {
      synchronized (this)
      {
         if (data != null)
         {
            data.clear();
            data = null;
         }
      }
   }

   public void printDetails(StringBuffer sb, int indent)
   {
      Map tmp;
      synchronized (this)
      {
         tmp = data != null ? new HashMap(data) : null;
      }
      printDetailsInMap(sb, indent, tmp);
   }

   public void printLockInfo(StringBuffer sb, int indent)
   {
      boolean locked = lock_ != null && lock_.isLocked();

      printIndent(sb, indent);
      sb.append(Fqn.SEPARATOR).append(getName());
      if (locked)
      {
         sb.append("\t(");
         lock_.toString(sb);
         sb.append(")");
      }

      if (children != null && children.size() > 0)
      {
         Collection values = children.values();
         for (Iterator it = values.iterator(); it.hasNext();)
         {
            sb.append("\n");
            ((DataNode) it.next()).printLockInfo(sb, indent + INDENT);
         }
      }
   }

   /**
    * Returns a debug string.
    */
   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append("\nfqn=" + fqn);
      synchronized (this)
      {
         if (data != null)
         {
            if (trace)
            {
               sb.append("\ndata=").append(data.keySet());
            }
            else
            {
               sb.append("\ndata=[");
               Set keys = data.keySet();
               int i=0;
               for (Iterator it = keys.iterator(); it.hasNext();)
               {
                  i++;
                  sb.append(it.next());

                  if (i == 5)
                  {
                     int more = keys.size() - 5;
                     if (more > 1)
                     {
                        sb.append(", and ");
                        sb.append(more);
                        sb.append(" more");
                        break;
                     }
                  }
                  else
                  {
                     sb.append(", ");
                  }
               }
            }
            sb.append("]");
         }
      }
      if (lock_ != null)
      {
         sb.append("\n read locked=").append(isReadLocked());
         sb.append("\n write locked=").append(isWriteLocked());
      }
      return sb.toString();
   }

   public Object clone() throws CloneNotSupportedException
   {
      DataNode parent = (DataNode) getParent();
      DataNode n = (DataNode) NodeFactory.getInstance().createNodeOfType(parent, getName(), fqn, parent != null ? (DataNode) parent.clone() : null, data, cache);
      n.setChildren(children == null ? null : (HashMap) ((HashMap) children).clone());
      return n;
   }

   public boolean acquire(Object caller, long timeout, int lock_type) throws LockingException, TimeoutException, InterruptedException
   {
      // Note that we rely on IdentityLock for synchronization
      try
      {
         if (lock_type == LOCK_TYPE_NONE)
            return true;
         else if (lock_type == LOCK_TYPE_READ)
            return acquireReadLock(caller, timeout);
         else
            return acquireWriteLock(caller, timeout);
      }
      catch (UpgradeException e)
      {
         StringBuffer buf = new StringBuffer("failure upgrading lock: fqn=").append(fqn).append(", caller=").append(caller).
                 append(", lock=").append(lock_.toString(true));
         if (trace)
            log.trace(buf.toString());
         throw new UpgradeException(buf.toString(), e);
      }
      catch (LockingException e)
      {
         StringBuffer buf = new StringBuffer("failure acquiring lock: fqn=").append(fqn).append(", caller=").append(caller).
                 append(", lock=").append(lock_.toString(true));
         if (trace)
            log.trace(buf.toString());
         throw new LockingException(buf.toString(), e);
      }
      catch (TimeoutException e)
      {
         StringBuffer buf = new StringBuffer("failure acquiring lock: fqn=").append(fqn).append(", caller=").append(caller).
                 append(", lock=").append(lock_.toString(true));
         if (trace)
            log.trace(buf.toString());
         throw new TimeoutException(buf.toString(), e);
      }
   }

   protected boolean acquireReadLock(Object caller, long timeout) throws LockingException, TimeoutException, InterruptedException
   {
      initLock();
      if (trace)
      {
         log.trace(new StringBuffer("acquiring RL: fqn=").append(fqn).append(", caller=").append(caller).
                 append(", lock=").append(lock_.toString(DataNode.PRINT_LOCK_DETAILS)));
      }
      boolean flag = lock_.acquireReadLock(caller, timeout);
      if (trace)
      {
         log.trace(new StringBuffer("acquired RL: fqn=").append(fqn).append(", caller=").append(caller).
                 append(", lock=").append(lock_.toString(DataNode.PRINT_LOCK_DETAILS)));
      }
      return flag;
   }

   protected boolean acquireWriteLock(Object caller, long timeout) throws LockingException, TimeoutException, InterruptedException
   {
      initLock();
      if (trace)
      {
         log.trace(new StringBuffer("acquiring WL: fqn=").append(fqn).append(", caller=").append(caller).
                 append(", lock=").append(lock_.toString(DataNode.PRINT_LOCK_DETAILS)));
      }
      boolean flag = lock_.acquireWriteLock(caller, timeout);
      if (trace)
      {
         log.trace(new StringBuffer("acquired WL: fqn=").append(fqn).append(", caller=").append(caller).
                 append(", lock=").append(lock_.toString(DataNode.PRINT_LOCK_DETAILS)));
      }
      return flag;
   }

   public Set acquireAll(Object caller, long timeout, int lock_type) throws LockingException, TimeoutException, InterruptedException
   {
      DataNode tmp;
      boolean acquired;
      Set retval = new HashSet();

      if (lock_type == LOCK_TYPE_NONE)
         return retval;
      acquired = acquire(caller, timeout, lock_type);
      if (acquired)
         retval.add(getLock());

      if (children != null)
      {
         for (Iterator it = children.values().iterator(); it.hasNext();)
         {
            tmp = (DataNode) it.next();
            retval.addAll(tmp.acquireAll(caller, timeout, lock_type));
         }
      }
      return retval;
   }

   public void release(Object caller)
   {
      if (lock_ != null)
      {
         if (trace)
         {
            boolean wOwner = lock_.isWriteLocked() && lock_.getWriterOwner().equals(caller);
            log.trace("releasing " + (wOwner ? "WL" : "RL") + ": fqn=" + fqn + ", caller=" + caller);
         }
         lock_.release(caller);
         if (trace)
         {
            boolean wOwner = lock_.isWriteLocked() && lock_.getWriterOwner().equals(caller);
            log.trace("released " + (wOwner ? "WL" : "RL") + ": fqn=" + fqn + ", caller=" + caller);
         }
      }
   }

   public void releaseForce()
   {
      if (lock_ != null)
         lock_.releaseForce();
   }

   public void releaseAll(Object owner)
   {
      DataNode tmp;
      if (children != null)
      {
         for (Iterator it = children.values().iterator(); it.hasNext();)
         {
            tmp = (DataNode) it.next();
            tmp.releaseAll(owner);
         }
      }
      release(owner);
   }

   public void releaseAllForce()
   {
      DataNode tmp;
      if (children != null)
      {
         for (Iterator it = children.values().iterator(); it.hasNext();)
         {
            tmp = (DataNode) it.next();
            tmp.releaseAllForce();
         }
      }
      releaseForce();
   }

   /**
    * Serializes this object to ObjectOutput.
    * Writes the {@link Fqn} elements, children as a Map, and data as a Map.
    * (This is no longer used within JBoss Cache.)
    *
    * @see org.jboss.cache.marshall.TreeCacheMarshaller
    */
   public void writeExternal(ObjectOutput out) throws IOException
   {
      // DO NOT CHANGE THE WIRE FORMAT OF THIS CLASS!!
      // The whole purpose of implementing writeExternal() is to
      // exchange data with 1.2.3 versions of JBC.  Either the wire
      // format stays the same, or we don't bother implementing Externalizable
      out.writeObject(getName());
      out.writeObject(fqn);
      out.writeObject(getParent());
      out.writeObject(children);
      synchronized (this)
      {
         out.writeObject(data);
      }
   }

   /**
    * Deserializes this object from ObjectInput.
    * (This is no longer used within JBoss Cache.)
    */
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      in.readObject(); // "name", which we discard
      fqn = (Fqn) in.readObject();
      in.readObject(); // "parent", which we discard
      children = (Map) in.readObject();
      data = (Map) in.readObject();
      // Note that we don't have a cache at this point, so our LockStrategy
      // is going to be configured based on whatever LockStrategyFactory's
      // static isolationLevel_ field is set to. This gets overridden
      // whenever a cache is assigned via setTreeCacheInstance
   }

   public void markForRemoval()
   {
      put(REMOVAL_MARKER, null);
      // mark children as well.
      Map children = getChildren(true);
      if (children != null && !children.isEmpty())
      {
         // traverse children
         Iterator i = children.values().iterator();
         while (i.hasNext())
         {
            ((Node) i.next()).markForRemoval();
         }
      }
   }

   public void unmarkForRemoval(boolean deep)
   {
      remove(REMOVAL_MARKER);
      // unmark children as well.
      if (deep)
      {
         Map children = getChildren(true);
         if (children != null && !children.isEmpty())
         {
            // traverse children
            Iterator i = children.values().iterator();
            while (i.hasNext())
            {
               ((Node) i.next()).unmarkForRemoval(true);
            }
         }
      }
   }

   public boolean isMarkedForRemoval()
   {
      return containsKey(REMOVAL_MARKER);
   }
}

