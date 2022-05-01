package org.jboss.cache.loader;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.TreeCache;
import org.jboss.cache.lock.StripedLock;
import org.jboss.cache.buddyreplication.BuddyManager;
import org.jboss.cache.marshall.RegionManager;
import org.jboss.invocation.MarshalledValueInputStream;
import org.jboss.invocation.MarshalledValueOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Simple file-based CacheLoader implementation. Nodes are directories, attributes of a node is a file in the directory
 *
 * @author Bela Ban
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @version $Id: FileCacheLoader.java 4089 2007-06-29 13:33:38Z gzamarreno $
 */
public class FileCacheLoader implements ExtendedCacheLoader
{
   File root = null;
   TreeCache cache = null;
   Log log = LogFactory.getLog(getClass());
   RegionManager manager;

   protected final StripedLock lock = new StripedLock();

   /**
    * HashMap<Object,List<Modification>>. List of open transactions. Note that this is purely transient, as
    * we don't use a log, recovery is not available
    */
   Map transactions = new ConcurrentHashMap();

   /**
    * TreeCache data file.
    */
   public static final String DATA = "data.dat";

   /**
    * TreeCache directory suffix.
    */
   public static final String DIR_SUFFIX = "fdb";

   public FileCacheLoader()
   {
   }

   public void setConfig(Properties props)
   {
      String location = props != null ? props.getProperty("location") : null;
      if (location != null && location.length() > 0)
         root = new File(location);
   }

   public void setCache(TreeCache c)
   {
      cache = c;
   }

   public void create() throws Exception
   {
      lock.acquireLock(Fqn.ROOT, true);
      try
      {
         if (root == null)
         {
            String tmpLocation = System.getProperty("java.io.tmpdir", "C:\\tmp");
            root = new File(tmpLocation);
         }
         if (!root.exists())
         {
            if (log.isTraceEnabled())
               log.trace("Creating cache loader location " + root);
            boolean created = root.mkdirs();
            if (!created)
               throw new IOException("Unable to create cache loader location " + root);
         }

         if (!root.isDirectory())
            throw new IOException("Cache loader location [" + root + "] is not a directory!");
      }
      finally
      {
         lock.releaseLock(Fqn.ROOT);
      } 
   }

   public void start() throws Exception
   {
   }

   public void stop()
   {
   }

   public void destroy()
   {
   }


   public Set getChildrenNames(Fqn fqn) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {
         File parent = getDirectory(fqn, false);
         if (parent == null) return null;
         File[] children = parent.listFiles();
         HashSet s = new HashSet();
         for (int i = 0; i < children.length; i++)
         {
            File child = children[i];
            if (child.isDirectory() && child.getName().endsWith(DIR_SUFFIX))
            {
               String child_name = child.getName();
               child_name = child_name.substring(0, child_name.lastIndexOf(DIR_SUFFIX) - 1);
               s.add(child_name);
            }
         }
         return s.size() == 0 ? null : s;
      }
      finally
      {
         lock.releaseLock(fqn);
      }      
   }

   // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

//   public Object get(Fqn fqn, Object key) throws Exception {
//      Map m=loadAttributes(fqn);
//      if(m == null) return null;
//      return m.get(key);
//   }

   public Map get(Fqn fqn) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {
         return loadAttributes(fqn);
//      Map m=loadAttributes(fqn);
//      if(m == null || m.size() == 0) return null;
//      return m;
      }
      finally
      {
         lock.releaseLock(fqn);
      }
   }

   public boolean exists(Fqn fqn) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {
         File f = getDirectory(fqn, false);
         return f != null;
      }
      finally
      {
         lock.releaseLock(fqn);
      }      
   }

   public Object put(Fqn fqn, Object key, Object value) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {
         Object retval;
         Map m = loadAttributes(fqn);
         if (m == null) m = new HashMap();
         retval = m.put(key, value);
         storeAttributes(fqn, m);
         return retval;
      }
      finally
      {
         lock.releaseLock(fqn);
      }
   }

   public void put(Fqn fqn, Map attributes) throws Exception
   {
      put(fqn, attributes, false);
   }


   public void put(Fqn fqn, Map attributes, boolean erase) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {
         Map m = erase ? new HashMap() : loadAttributes(fqn);
         if (m == null) m = new HashMap();
         if (attributes != null)
            m.putAll(attributes);
         storeAttributes(fqn, m);
      }
      finally
      {
         lock.releaseLock(fqn);
      }
   }

   void put(Fqn fqn) throws Exception
   {
      getDirectory(fqn, true);
   }

   /**
    * @param modifications List<Modification>
    * @throws Exception
    */
   public void put(List modifications) throws Exception
   {
      if (modifications == null) return;
      for (Iterator it = modifications.iterator(); it.hasNext();)
      {
         Modification m = (Modification) it.next();
         switch (m.getType())
         {
            case Modification.PUT_DATA:
               put(m.getFqn(), m.getData());
               break;
            case Modification.PUT_DATA_ERASE:
               put(m.getFqn(), m.getData(), true);
               break;
            case Modification.PUT_KEY_VALUE:
               put(m.getFqn(), m.getKey(), m.getValue());
               break;
            case Modification.REMOVE_DATA:
               removeData(m.getFqn());
               break;
            case Modification.REMOVE_KEY_VALUE:
               remove(m.getFqn(), m.getKey());
               break;
            case Modification.REMOVE_NODE:
               remove(m.getFqn());
               break;
            default:
               log.error("modification type " + m.getType() + " not known");
               break;
         }
      }
   }

   public Object remove(Fqn fqn, Object key) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {
         Object retval;
         Map m = loadAttributes(fqn);
         if (m == null) return null;
         retval = m.remove(key);
         storeAttributes(fqn, m);
         return retval;
      }
      finally
      {
         lock.releaseLock(fqn);
      }         
   }

   public void remove(Fqn fqn) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {
         File dir = getDirectory(fqn, false);
         if (dir != null)
         {
            boolean flag = removeDirectory(dir, true);
            if (!flag)
               log.warn("failed removing " + fqn);
         }
      }
      finally
      {
         lock.releaseLock(fqn);
      }
   }

   public void removeData(Fqn fqn) throws Exception
   {
      lock.acquireLock(fqn, true);
      try
      {      
         File f = getDirectory(fqn, false);
         if (f != null)
         {
            File data = new File(f, DATA);
            if (data.exists())
            {
               boolean flag = data.delete();
               if (!flag)
                  log.warn("failed removing file " + data.getName());
            }
         }
      }
      finally
      {
         lock.releaseLock(fqn);
      }      
   }

   public void prepare(Object tx, List modifications, boolean one_phase) throws Exception
   {
      if (one_phase)
         put(modifications);
      else
         transactions.put(tx, modifications);
   }

   public void commit(Object tx) throws Exception
   {
      List modifications = (List) transactions.remove(tx);
      if (modifications == null)
         throw new Exception("transaction " + tx + " not found in transaction table");
      put(modifications);
   }

   public void rollback(Object tx)
   {
      transactions.remove(tx);
   }

   /**
    * Loads the entire state from the filesystem and returns it as a byte buffer. The format of the byte buffer
    * must be a list of NodeData elements
    *
    * @return
    * @throws Exception
    */
   public byte[] loadEntireState() throws Exception
   {
      return loadState(Fqn.ROOT);
   }

   public byte[] loadState(Fqn subtree) throws Exception
   {
      ClassLoader currentCL = Thread.currentThread().getContextClassLoader();

      try
      {
         // Set the TCCL to any classloader registered for subtree
         setUnmarshallingClassLoader(subtree);

         ByteArrayOutputStream out_stream = new ByteArrayOutputStream(1024);
         ObjectOutputStream out = new MarshalledValueOutputStream(out_stream);
         loadStateFromFilessystem(subtree, out);
         out.close();

         return out_stream.toByteArray();
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(currentCL);
      }
   }

   /**
    * Stores the state given as a byte buffer to the filesystem. The byte
    * buffer contains a list of zero or more NodeData elements
    *
    * @param state
    * @throws Exception
    */
   public void storeEntireState(byte[] state) throws Exception
   {
      storeState(state, Fqn.ROOT);
   }

   public void storeState(byte[] state, Fqn subtree) throws Exception
   {
      ClassLoader currentCL = Thread.currentThread().getContextClassLoader();
      try
      {
         // Set the TCCL to any classloader registered for subtree
         setUnmarshallingClassLoader(subtree);

         ByteArrayInputStream in_stream = new ByteArrayInputStream(state);
         MarshalledValueInputStream in = new MarshalledValueInputStream(in_stream);
         NodeData nd;

         // remove entire existing state
         this.remove(subtree);

         boolean moveToBuddy =
                 subtree.isChildOf(BuddyManager.BUDDY_BACKUP_SUBTREE_FQN) && subtree.size() > 1;

         // store new state
         Fqn fqn;
         while (in_stream.available() > 0)
         {
            nd = (NodeData) in.readObject();

            if (moveToBuddy)
               fqn = BuddyManager.getBackupFqn(subtree, nd.fqn);
            else
               fqn = nd.fqn;

            if (nd.attrs != null)
               this.put(fqn, nd.attrs, true); // creates a node with 0 or more attributes
            else
               this.put(fqn);  // creates a node with null attributes
         }
      }
      finally
      {
         Thread.currentThread().setContextClassLoader(currentCL);
      }
   }

   public void setRegionManager(RegionManager manager)
   {
      this.manager = manager;
   }

   /* ----------------------- Private methods ------------------------ */


   /**
    * Do a preorder traversal: visit the node first, then the node's children
    *
    * @param fqn Start node
    * @param out
    * @throws Exception
    */
   protected void loadStateFromFilessystem(Fqn fqn, ObjectOutputStream out) throws Exception
   {
      Map attrs;
      Set children_names;
      String child_name;
      Fqn tmp_fqn;
      NodeData nd;

      // first handle the current node
      attrs = get(fqn);
      if (attrs == null || attrs.size() == 0)
         nd = new NodeData(fqn);
      else
         nd = new NodeData(fqn, attrs);
      out.writeObject(nd);

      // then visit the children
      children_names = getChildrenNames(fqn);
      if (children_names == null)
         return;
      for (Iterator it = children_names.iterator(); it.hasNext();)
      {
         child_name = (String) it.next();
         tmp_fqn = new Fqn(fqn, child_name);
         loadStateFromFilessystem(tmp_fqn, out);
      }
   }


   File getDirectory(Fqn fqn, boolean create)
   {
      File f = new File(getFullPath(fqn));
      if (!f.exists())
      {
         if (create)
            f.mkdirs();
         else
            return null;
      }
      return f;
   }


   /**
    * Recursively removes this and all subdirectories, plus all DATA files in them. To prevent damage, we only
    * remove files that are named DATA (data.dat) and directories which end in ".fdb". If there is a dir or file
    * that isn't named this way, the recursive removal will fail
    *
    * @return <code>true</code> if directory was removed,
    *         <code>false</code> if not.
    */
   boolean removeDirectory(File dir, boolean include_start_dir)
   {
      boolean success = true;
      File[] subdirs = dir.listFiles();
      for (int i = 0; i < subdirs.length; i++)
      {
         File file = subdirs[i];
         if (file.isFile() && file.getName().equals(DATA))
         {
            if (!file.delete())
               success = false;
            continue;
         }
         if (file.isDirectory() && file.getName().endsWith(DIR_SUFFIX))
         {
            if (!removeDirectory(file, false))
               success = false;
            if (!file.delete())
               success = false;
         }
      }

      if (include_start_dir)
      {
         if (!dir.equals(root))
         {
            if (dir.delete())
            {return success;}
            success = false;
         }
      }

      return success;
   }

   String getFullPath(Fqn fqn)
   {
      StringBuffer sb = new StringBuffer(root.getAbsolutePath() + File.separator);
      for (int i = 0; i < fqn.size(); i++)
      {
         Object tmp = fqn.get(i);
         String tmp_dir;
         if (tmp instanceof String)
            tmp_dir = (String) tmp;
         else
            tmp_dir = tmp.toString();
         sb.append(tmp_dir).append(".").append(DIR_SUFFIX).append(File.separator);
      }
      return sb.toString();
   }

   protected Map loadAttributes(Fqn fqn) throws Exception
   {
      File f = getDirectory(fqn, false);
      if (f == null) return null; // i.e., this node does not exist.
      // this node exists so we should never return a null after this... at worst case, an empty HashMap.
      File child = new File(f, DATA);
      if (!child.exists()) return new HashMap(0); // no node attribs exist hence the empty HashMap.
      //if(!child.exists()) return null;
      FileInputStream in = new FileInputStream(child);
      MarshalledValueInputStream input = new MarshalledValueInputStream(in);
      Map m = (Map) input.readObject();
      in.close();
      return m;
   }

   protected void storeAttributes(Fqn fqn, Map attrs) throws Exception
   {
      File f = getDirectory(fqn, true);
      File child = new File(f, DATA);
      if (!child.exists())
         if (!child.createNewFile())
            throw new IOException("Unable to create file: " + child);
      FileOutputStream out = new FileOutputStream(child);
      ObjectOutputStream output = new ObjectOutputStream(out);
      output.writeObject(attrs);
      out.close();
   }

   /**
    * Checks the RegionManager for a classloader registered for the
    * given, and if found sets it as the TCCL
    *
    * @param subtree
    */
   private void setUnmarshallingClassLoader(Fqn subtree)
   {
      if (manager != null)
      {
         manager.setUnmarshallingClassLoader(subtree);
      }
   }

}
