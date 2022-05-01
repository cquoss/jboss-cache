/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.TreeCache;
import org.jboss.cache.lock.StripedLock;
import org.jboss.invocation.MarshalledValue;
import org.jboss.invocation.MarshalledValueInputStream;
import org.jboss.invocation.MarshalledValueOutputStream;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.*;
import java.rmi.MarshalledObject;
import java.sql.*;
import java.util.*;

/**
 * JDBC CacheLoader implementation.
 * <p/>
 * This implementation uses one table. The table consists of three columns:
 * <ul>
 * <li>text column for fqn (which is also a primary key)</li>
 * <li>blob column for attributes (can contain null)</li>
 * <li>text column for parent fqn (can contain null)</li>
 * </ul>
 * <p/>
 * The configuration options are:
 * <p/>
 * <b>Table configuration</b>
 * <ul>
 * <li><b>cache.jdbc.table.name</b> - the table name (default is <i>jbosscache</i>)</li>
 * <li><b>cache.jdbc.table.create</b> - should be true or false, indicates whether to create the table at start phase</li>
 * <li><b>cache.jdbc.table.drop</b> - should be true or false, indicates whether to drop the table at stop phase</li>
 * <li><b>cache.jdbc.table.primarykey</b> - the name for the table primary key (default is <i>jbosscache_pk</i>)</li>
 * <li><b>cache.jdbc.fqn.column</b> - the name for the fqn column (default is <i>fqn</i>)</li>
 * <li><b>cache.jdbc.fqn.type</b> - the type for the fqn column (default is <i>varchar(255)</i>)</li>
 * <li><b>cache.jdbc.node.column</b> - the name for the node's contents column (default is <i>node</i>)</li>
 * <li><b>cache.jdbc.node.type</b> - the type for the node's contents column (default is <i>blob</i>)</li>
 * <li><b>cache.jdbc.parent.column</b> - the name for the parent fqn column (default is <i>parent</i>)</li>
 * </ul>
 * <p/>
 * <b>DataSource configuration</b>
 * <ul>
 * <li><b>cache.jdbc.datasource</b> - the JNDI name of the datasource</li>
 * </ul>
 * <p/>
 * <b>JDBC driver configuration (used when DataSource is not configured)</b>
 * <ul>
 * <li><b>cache.jdbc.driver</b> - fully qualified JDBC driver name</li>
 * <li><b>cache.jdbc.url</b> - URL to connect to the database</li>
 * <li><b>cache.jdbc.user</b> - the username to use to connect to the database</li>
 * <li><b>cache.jdbc.password</b> - the password to use to connect to the database</li>
 * </ul>
 *
 * @author <a href="mailto:alex@jboss.org">Alexey Loubyansky</a>
 * @author <a href="mailto:hmesha@novell.com">Hany Mesha </a>
 * @version <tt>$Revision: 4089 $</tt>
 */
public class JDBCCacheLoader
   implements CacheLoader
{
   private static final Log log=LogFactory.getLog(JDBCCacheLoader.class);

   private static final ThreadLocal connection = new ThreadLocal();
   
   private String driverName;

   private String drv;
   private String table;
   private String selectChildNamesSql;
   private String deleteNodeSql;
   private String deleteAllSql;
   private String selectChildFqnsSql;
   private String insertNodeSql;
   private String updateNodeSql;
   private String selectNodeSql;
   private String createTableDdl;
   private String dropTableDdl;

   private boolean createTable;
   private boolean dropTable;

   private String datasourceName;
   private ConnectionFactory cf;

   protected StripedLock lock = new StripedLock();

   public void setConfig(Properties props)
   {
      datasourceName = props.getProperty("cache.jdbc.datasource");
      if(datasourceName == null)
      {
         this.drv = getRequiredProperty(props, "cache.jdbc.driver");
         final String jdbcUrl = getRequiredProperty(props, "cache.jdbc.url");
         final String jdbcUsr = getRequiredProperty(props, "cache.jdbc.user");
         final String jdbcPwd = getRequiredProperty(props, "cache.jdbc.password");

         if(log.isDebugEnabled())
         {
            log.debug("Properties: " +
               "cache.jdbc.url=" +
               jdbcUrl +
               ", cache.jdbc.driver=" +
               drv +
               ", cache.jdbc.user=" +
               jdbcUsr +
               ", cache.jdbc.password=" +
               jdbcPwd +
               ", cache.jdbc.table=" + table);
         }

         this.cf = new NonManagedConnectionFactory(jdbcUrl, jdbcUsr, jdbcPwd);
      }
      // else we wait until the start method to do a JNDI lookup
      // of the datasource, since that's when its registered in its lifecycle

      String prop = props.getProperty("cache.jdbc.table.create");
      this.createTable = (prop == null || Boolean.valueOf(prop).booleanValue());
      prop = props.getProperty("cache.jdbc.table.drop");
      this.dropTable = (prop == null || Boolean.valueOf(prop).booleanValue());

      this.table = props.getProperty("cache.jdbc.table.name", "jbosscache");
      String primaryKey =props.getProperty("cache.jdbc.table.primarykey", "jbosscache_pk");
      String fqnColumn = props.getProperty("cache.jdbc.fqn.column", "fqn");
      String fqnType = props.getProperty("cache.jdbc.fqn.type", "varchar(255)");
      String nodeColumn = props.getProperty("cache.jdbc.node.column", "node");
      String nodeType = props.getProperty("cache.jdbc.node.type", "blob");
      String parentColumn = props.getProperty("cache.jdbc.parent.column", "parent");

      selectChildNamesSql = "select " + fqnColumn + " from " + table + " where " + parentColumn + "=?";
      deleteNodeSql = "delete from " + table + " where " + fqnColumn + "=?";
      deleteAllSql = "delete from " + table;
      selectChildFqnsSql = "select " + fqnColumn + " from " + table + " where " + parentColumn + "=?";
      insertNodeSql = "insert into " +
         table +
         " (" +
         fqnColumn +
         ", " +
         nodeColumn +
         ", " +
         parentColumn +
         ") values (?, ?, ?)";
      updateNodeSql = "update " + table + " set " + nodeColumn + "=? where " + fqnColumn + "=?";
      selectNodeSql = "select " + nodeColumn + " from " + table + " where " + fqnColumn + "=?";

      createTableDdl = "create table " +
         table +
         "(" +
         fqnColumn +
         " " +
         fqnType +
         " not null, " +
         nodeColumn +
         " " +
         nodeType +
         ", " +
         parentColumn +
         " " +
         fqnType +
         ", constraint " + primaryKey + " primary key (" + fqnColumn + "))";

      dropTableDdl = "drop table " + table;
   }

   public void setCache(TreeCache c)
   {
      //todo setCache(TreeCache c)
   }

   /**
    * Fetches child node names (not pathes).
    *
    * @param fqn parent fqn
    * @return a set of child node names or null if there are not children found for the fqn
    * @throws Exception
    */
   public Set getChildrenNames(Fqn fqn) throws Exception
   {
      Set children = null;
      Connection con = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try
      {
         if(log.isDebugEnabled())
         {
            log.debug("executing sql: " + selectChildNamesSql + " (" + fqn + ")");
         }

         con = cf.getConnection();
         ps = con.prepareStatement(selectChildNamesSql);
         ps.setString(1, fqn.toString());
         lock.acquireLock(fqn, false);
         rs = ps.executeQuery();
         if(rs.next())
         {
            children = new HashSet();
            do
            {
               String child = rs.getString(1);
               int slashInd = child.lastIndexOf('/');
               String name = child.substring(slashInd + 1);
               //Fqn childFqn = Fqn.fromString(child);
               //String name = (String) childFqn.get(childFqn.size() - 1);
               children.add(name);
            }
            while(rs.next());
         }
      }
      catch(SQLException e)
      {
         log.error("Failed to get children names for fqn " + fqn, e);
         throw new IllegalStateException("Failed to get children names for fqn " + fqn + ": " + e.getMessage());
      }
      finally
      {
         safeClose(rs);
         safeClose(ps);
         cf.close(con);
         lock.releaseLock(fqn);
      }

      return children == null ? null : Collections.unmodifiableSet(children);
   }

    // See http://jira.jboss.com/jira/browse/JBCACHE-118 for why this is commented out.

    /**
    * Loads an attribute from the database.
    *
    * @param name node's fqn
    * @param key  attribute's key
    * @return attribute's value. Null is returned if
    *         <ul>
    *         <li>there is no value for the attribute key in the node</li>
    *         <li>there is a row in the table for the fqn but the node column contains null</li>
    *         <li>there is no row in table for the fqn (should this result in an exception?)</li>
    *         </ul>
    * @throws Exception
    */
//   public Object get(Fqn name, Object key) throws Exception
//   {
//      Map node = loadNode(name);
//      return node == null || node == NULL_NODE_IN_ROW ? null : node.get(key);
//   }

   /**
    * Returns a map representing a node.
    *
    * @param name node's fqn
    * @return node
    * @throws Exception
    */
   public Map get(Fqn name) throws Exception
   {
      lock.acquireLock(name, false);
      try
      {
         final Map node = loadNode(name);
         return node == NULL_NODE_IN_ROW ? new HashMap(0) : node;
      }
      finally
      {
         lock.releaseLock(name);         
      }
   }

   /**
    * Checks that there is a row for the fqn in the database.
    *
    * @param name node's fqn
    * @return true if there is a row in the database for the given fqn even if the node column is null.
    * @throws Exception
    */
   public boolean exists(Fqn name) throws Exception
   {
      lock.acquireLock(name, false);
      try
      {
         final Map node = loadNode(name);
         return node != null;// && node != NULL_NODE_IN_ROW;
      }
      finally
      {
         lock.releaseLock(name);
      }         
   }

   /**
    * Adds/overrides a value in a node for a key.
    * If the node does not exist yet, the node will be created.
    * If parent nodes do not exist for the node, empty parent nodes will be created.
    *
    * @param name  node's fqn
    * @param key   attribute's key
    * @param value attribute's value
    * @return old value associated with the attribute's key or null if there was no value previously
    *         associated with the attribute's key
    * @throws Exception
    */
   public Object put(Fqn name, Object key, Object value) throws Exception
   {
      lock.acquireLock(name, true);
      try
      {
         Map oldNode = loadNode(name);
         Object oldValue;
         Map node;

         if(oldNode == null || oldNode == NULL_NODE_IN_ROW)
         {
            node = new HashMap();
         }
         else
         {
            node = oldNode;
         }
         oldValue = node.put(key, value);

         if(oldNode != null)
         {
            updateNode(name, node);
         }
         else
         {
            if(name.size() > 1)
            {
               for(int i = 1; i < name.size(); ++i)
               {
                  final Fqn parent = name.getFqnChild(i);
                  if(!exists(parent))
                  {
                     insertNode(parent, null);
                  }
               }
            }
            insertNode(name, node);
         }

         return oldValue;
      }
      finally
      {
         lock.releaseLock(name);
      }      
   }

   /**
    * Adds attributes from the passed in map to the existing node.
    * If there is no node for the fqn, a new node will be created.
    *
    * @param name       node's fqn
    * @param attributes attributes
    * @throws Exception
    */
   public void put(Fqn name, Map attributes) throws Exception
   {
      put(name, attributes, false);
   }

   public void put(List modifications) throws Exception
   {
      for(int i = 0; i < modifications.size(); ++i)
      {
         Modification m = (Modification) modifications.get(i);
         switch(m.getType())
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
               throw new IllegalStateException("Unexpected modification code: " + m.getType());
         }
      }
   }

   /**
    * Removes attribute's value for a key. If after removal the node contains no attributes, the node is nullified.
    *
    * @param name node's name
    * @param key  attribute's key
    * @return removed value or null if there was no value for the passed in key
    * @throws Exception
    */
   public Object remove(Fqn name, Object key) throws Exception
   {
      lock.acquireLock(name, true);
      try
      {
         Object removedValue = null;
         Map node = loadNode(name);
         if(node != null && node != NULL_NODE_IN_ROW)
         {
            removedValue = node.remove(key);
            if(node.isEmpty())
            {
               updateNode(name, null);
            }
            else
            {
               updateNode(name, node);
            }
         }
         return removedValue;
      }
      finally
      {
         lock.releaseLock(name);
      }         
   }

   /**
    * Removes a node and all its children.
    * Uses the same connection for all the db work.
    *
    * @param name node's fqn
    * @throws Exception
    */
   public void remove(Fqn name) throws Exception
   {
      Connection con = null;
      PreparedStatement ps = null;
      try
      {
         if(name.size() == 0)
         {
            if(log.isDebugEnabled())
            {
               log.debug("executing sql: " + deleteAllSql);
            }

            con = cf.getConnection();
            ps = con.prepareStatement(deleteAllSql);
            lock.acquireLock(name, true);
            int deletedRows = ps.executeUpdate();

            if(log.isDebugEnabled())
            {
               log.debug("total rows deleted: " + deletedRows);
            }
         }
         else
         {
            StringBuffer sql = new StringBuffer(300);
             sql.append("delete from ").append(table).append(" where fqn in (");
            //sql2.append("delete from " + table + " where fqn=? or parent in (");
            List fqns = new ArrayList();

            addChildrenToDeleteSql(name.toString(), sql, fqns);

            sql.append(')');

            if(fqns.size() == 1)
            {
               if(log.isDebugEnabled())
               {
                  log.debug("executing sql: " + deleteNodeSql + "(" + name + ")");
               }

               con = cf.getConnection();
               ps = con.prepareStatement(deleteNodeSql);
               ps.setString(1, name.toString());
            }
            else
            {
               if(log.isDebugEnabled())
               {
                  log.debug("executing sql: " + sql + " " + fqns);
               }

               con = cf.getConnection();
               ps = con.prepareStatement(sql.toString());
               for(int i = 0; i < fqns.size(); ++i)
               {
                  ps.setString(i + 1, (String) fqns.get(i));
               }
            }

            lock.acquireLock(name, true);
            int deletedRows = ps.executeUpdate();

            if(log.isDebugEnabled())
            {
               log.debug("total rows deleted: " + deletedRows);
            }
         }
      }
      catch(SQLException e)
      {
         log.error("Failed to remove node " + name, e);
         throw new IllegalStateException("Failed to remove node " + name + ": " + e.getMessage());
      }
      finally
      {
         safeClose(ps);
         cf.close(con);
         lock.releaseLock(name);
      }
   }

   /**
    * Nullifies the node.
    *
    * @param name node's fqn
    * @throws Exception
    */
   public void removeData(Fqn name) throws Exception
   {
      updateNode(name, null);
   }

   /**
    * First phase in transaction commit process. The changes are committed if only one phase if requested.
    * All the modifications are committed using the same connection.
    *
    * @param tx            something representing transaction
    * @param modifications a list of modifications
    * @param one_phase     indicates whether it's one or two phase commit transaction
    * @throws Exception
    */
   public void prepare(Object tx, List modifications, boolean one_phase) throws Exception
   {
      // start a tx
      //JBCACHE-346 fix, we don't need to prepare a DataSource object (Managed connection)
      if(cf instanceof NonManagedConnectionFactory) {
         Connection con = cf.prepare(tx);
         if(log.isTraceEnabled())
         {
            log.trace("openned tx connection: tx=" + tx + ", con=" + con);
         }
      }      

      try
      {
         put(modifications);

         // commit if it's one phase only
         if(one_phase)
         {
            commit(tx);
         }
      }
      catch(Exception e)
      {
         // todo should I rollback it here or rollback is supposed to be invoke by someone from outside?
         rollback(tx);
         // is this ok?
         throw e;
      }
   }

   /**
    * Commits a transaction.
    *
    * @param tx the tx to commit
    * @throws Exception
    */
   public void commit(Object tx) throws Exception
   {
      cf.commit(tx);
   }

   /**
    * Rolls back a transaction.
    *
    * @param tx the tx to rollback
    */
   public void rollback(Object tx)
   {
      cf.rollback(tx);
   }

   /**
    * WARN: this was copied from other cache loader implementation
    *
    * @return
    * @throws Exception
    */
/*
   public byte[] loadEntireState() throws Exception
   {
      ByteArrayOutputStream out_stream = new ByteArrayOutputStream(1024);
      ObjectOutputStream out = new ObjectOutputStream(out_stream);
      loadState(Fqn.fromString("/"), out);
      out.close();
      return out_stream.toByteArray();
   }
*/

   /**
    * Loads the entire state from the filesystem and returns it as a byte buffer. The format of the byte buffer
    * must be a list of NodeData elements
    * @return
    * @throws Exception
    */
   public byte[] loadEntireState() throws Exception {
      ByteArrayOutputStream out_stream=new ByteArrayOutputStream(1024);
      ObjectOutputStream    out=new MarshalledValueOutputStream(out_stream);
      loadState(Fqn.fromString("/"), out);
      out.close();
      return out_stream.toByteArray();
   }


   /**
    * WARN: this was copied from other cache loader implementation
    *
    * @param state
    * @throws Exception
    */
/*   public void storeEntireState(byte[] state) throws Exception
   {
      Fqn fqn = null;
      Map map;
      int num_attrs = 0;
      ByteArrayInputStream in_stream = new ByteArrayInputStream(state);
      MarshalledValueInputStream in = new MarshalledValueInputStream(in_stream);

      // remove previous state
      this.remove(Fqn.fromString("/"));

      // store new state
      try
      {
         while(true)
         {
            map = null;
            fqn = (Fqn) in.readObject();
            num_attrs = in.readInt();
            if(num_attrs > -1)
            {
               map = (Map) in.readObject();
            }
            if(map != null)
            {
               this.put(fqn, map, true); // creates a node with 0 or more attributes
            }
            else
            {
               this.put(fqn, null);  // creates a node with null attributes
            }
         }
      }
      catch(EOFException eof_ex)
      {
      }
   }*/


   /** Store the state given as a byte buffer to the database. The byte buffer contains a list
    * of zero or more NodeData elements
    * @param state
    * @throws Exception
    */
   public void storeEntireState(byte[] state) throws Exception {
      ByteArrayInputStream in_stream=new ByteArrayInputStream(state);
      MarshalledValueInputStream in=new MarshalledValueInputStream(in_stream);
      NodeData nd;

      // remove entire existing state
      this.remove(Fqn.fromString("/"));

      // store new state
      try {
         while(true) {
            nd=(NodeData) in.readObject();
            if(nd.attrs != null)
               this.put(nd.fqn, nd.attrs, true); // creates a node with 0 or more attributes
            else
               this.put(nd.fqn, null);  // creates a node with null attributes
         }
      }
      catch(EOFException eof_ex) {
      }
   }



   // Service implementation

   public void create() throws Exception
   {
   }

   public void start() throws Exception
   {
      if(drv != null)
      {
         loadDriver(drv);
      }
      else
      {
         // A datasource will be registered in JNDI in the start portion of
         // its lifecycle, so now that we are in start() we can look it up
         InitialContext ctx = null;
         try
         {
            ctx = new InitialContext();
            DataSource dataSource = (DataSource) ctx.lookup(datasourceName);
            this.cf = new ManagedConnectionFactory(dataSource);
         }
         catch(NamingException e)
         {
            log.error("Failed to lookup datasource " + datasourceName + ": " + e.getMessage(), e);
            throw new IllegalStateException("Failed to lookup datasource " + datasourceName + ": " + e.getMessage());
         }
         finally
         {
            if(ctx != null)
            {
               try
               {
                  ctx.close();
               }
               catch(NamingException e)
               {
                  log.warn("Failed to close naming context.", e);
               }
            }
         }
      }
      
      Connection con = null;
      Statement st = null;
      
      try
      {
         con = cf.getConnection();
         driverName = getDriverName(con);
         if(createTable)
         {
            if(!tableExists(table, con))
            {
               if(log.isDebugEnabled())
               {
                  log.debug("executing ddl: " + createTableDdl);
               }
               st = con.createStatement();
               st.executeUpdate(createTableDdl);
            }
         }
      }
      finally
      {
         safeClose(st);
         cf.close(con);
      }
   }

   public void stop()
   {
      if(dropTable)
      {
         Connection con = null;
         Statement st = null;
         try
         {
            if(log.isDebugEnabled())
            {
               log.debug("executing ddl: " + dropTableDdl);
            }

            con = cf.getConnection();
            st = con.createStatement();
            st.executeUpdate(dropTableDdl);
            safeClose(st);
         }
         catch(SQLException e)
         {
            log.error("Failed to drop table: " + e.getMessage(), e);
         }
         finally
         {
            safeClose(st);
            cf.close(con);
         }
      }
   }

   public void destroy()
   {
   }

   // Private

   private void addChildrenToDeleteSql(String name, StringBuffer sql, List fqns)
      throws SQLException
   {
      // for now have to use connection per method, i.e. can't pass the same connection to recursive
      // invocations because buggy PointBase driver invalidates result sets.
      Connection con = null;
      PreparedStatement selChildrenPs = null;
      ResultSet rs = null;
      try
      {
         if(log.isDebugEnabled())
         {
            log.debug("executing sql: " + selectChildFqnsSql + "(" + name + ")");
         }

         con = cf.getConnection();
         selChildrenPs = con.prepareStatement(selectChildFqnsSql);
         selChildrenPs.setString(1, name);
         rs = selChildrenPs.executeQuery();

         if(rs.next())
         {
            do
            {
               String childStr = rs.getString(1);
               addChildrenToDeleteSql(childStr, sql, fqns);
            }
            while(rs.next());
         }

         if(fqns.size() == 0)
         {
            sql.append("?");
         }
         else
         {
            sql.append(", ?");
         }
         fqns.add(name);
      }
      finally
      {
         safeClose(rs);
         safeClose(selChildrenPs);
         cf.close(con);
      }
   }

/*
   void loadState(Fqn fqn, ObjectOutputStream out)
      throws Exception
   {
      Map attrs;
      Set children_names;
      String child_name;
      int num_attrs;
      Fqn tmp_fqn;

      children_names = getChildrenNames(fqn);
      attrs = get(fqn);
      num_attrs = attrs == null ? -1 : attrs.size();
      out.writeObject(fqn);
      out.writeInt(num_attrs);
      if(attrs != null)
      {
         out.writeObject(attrs);
      }

      if(children_names == null)
      {
         return;
      }

      for(Iterator it = children_names.iterator(); it.hasNext();)
      {
         child_name = (String) it.next();
         tmp_fqn = new Fqn(fqn, child_name);
         loadState(tmp_fqn, out);
      }
   }
*/

   /**
    * Do a preorder traversal: visit the node first, then the node's children
    * @param fqn Start node
    * @param out
    * @throws Exception
    */
   protected void loadState(Fqn fqn, ObjectOutputStream out) throws Exception {
      Map       attrs;
      Set       children_names;
      String    child_name;
      Fqn       tmp_fqn;
      NodeData  nd;

      // first handle the current node
      attrs=get(fqn);
      if(attrs == null || attrs.size() == 0)
         nd=new NodeData(fqn);
      else
         nd=new NodeData(fqn, attrs);
      out.writeObject(nd);

      // then visit the children
      children_names=getChildrenNames(fqn);
      if(children_names == null)
         return;
      for(Iterator it=children_names.iterator(); it.hasNext();) {
         child_name=(String)it.next();
         tmp_fqn=new Fqn(fqn, child_name);
         loadState(tmp_fqn, out);
      }
   }



   final void put(Fqn name, Map attributes, boolean override) throws Exception
   {
      // JBCACHE-769 -- make a defensive copy
      Map attrs = (attributes == null ? null : new HashMap(attributes));

      lock.acquireLock(name, true);
      try
      {
         Map oldNode = loadNode(name);
         if(oldNode != null)
         {
            if(!override && oldNode != NULL_NODE_IN_ROW && attrs != null)
            {
               attrs.putAll(oldNode);
            }
            updateNode(name, attrs);
         }
         else
         {
            if(name.size() > 1)
            {
               for(int i = 1; i < name.size(); ++i)
               {
                  final Fqn parent = name.getFqnChild(i);
                  if(!exists(parent))
                  {
                     insertNode(parent, null);
                  }
               }
            }
            insertNode(name, attrs);
         }
      }
      finally
      {
         lock.releaseLock(name);
      }
   }

   /**
    * Inserts a node into the database
    *
    * @param name the fqn
    * @param node the node
    */
   private void insertNode(Fqn name, Map node)
   {
      Connection con = null;
      PreparedStatement ps = null;
      try
      {
         if(log.isDebugEnabled())
         {
            log.debug("executing sql: " + insertNodeSql + " (" + name + ")");
         }

         con = cf.getConnection();
         ps = con.prepareStatement(insertNodeSql);

         ps.setString(1, name.toString());

         if(node != null)
         {
            Object marshalledNode = new MarshalledValue(node);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(marshalledNode);

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ps.setBinaryStream(2, bais, baos.size());
         }
         else
         {
            // a hack to handles the incomp. of SQL server jdbc driver prior to SQL SERVER 2005
            if( driverName != null && (driverName.indexOf("SQLSERVER") >= 0 
                                   || driverName.indexOf("POSTGRESQL") >= 0)) 
               ps.setNull( 2, Types.LONGVARBINARY ); 
            else 
               ps.setNull(2, Types.BLOB); 
            //ps.setNull(2, Types.LONGVARBINARY);
         }

         if(name.size() == 0)
         {
            ps.setNull(3, Types.VARCHAR);
         }
         else
         {
            ps.setString(3, name.getFqnChild(name.size() - 1).toString());
         }

         int rows = ps.executeUpdate();
         if(rows != 1)
         {
            throw new IllegalStateException("Expected one insert row but got " + rows);
         }
      }
      catch(RuntimeException e)
      {
         throw e;
      }
      catch(Exception e)
      {
         log.error("Failed to insert node: " + e.getMessage(), e);
         throw new IllegalStateException("Failed to insert node: " + e.getMessage());
      }
      finally
      {
         safeClose(ps);
         cf.close(con);
      }
   }

   /**
    * Updates a node in the database.
    *
    * @param name the fqn
    * @param node new node value
    */
   private void updateNode(Fqn name, Map node)
   {
      Connection con = null;
      PreparedStatement ps = null;
      try
      {
         if(log.isDebugEnabled())
         {
            log.debug("executing sql: " + updateNodeSql);
         }

         con = cf.getConnection();
         ps = con.prepareStatement(updateNodeSql);

         if(node == null)
         {
            //ps.setNull(1, Types.BLOB);
//            ps.setNull(1, Types.LONGVARBINARY);
             // don't set it to null - simply use an empty hash map.
             node = new HashMap(0);
         }
//         else
//         {
        Object marshalledNode = new MarshalledValue(node);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(marshalledNode);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ps.setBinaryStream(1, bais, baos.size());
//         }

         ps.setString(2, name.toString());

         int rows = ps.executeUpdate();
         if(rows != 1)
         {
            throw new IllegalStateException("Expected one updated row but got " + rows);
         }
      }
      catch(Exception e)
      {
         log.error("Failed to update node for fqn " + name + ": " + e.getMessage(), e);
         throw new IllegalStateException("Failed to update node for fqn " + name + ": " + e.getMessage());
      }
      finally
      {
         safeClose(ps);
         cf.close(con);
      }
   }

   /**
    * Loads a node from the database.
    *
    * @param name the fqn
    * @return non-null Map representing the node,
    *         null if there is no row with the fqn in the table,
    *         NULL_NODE_IN_ROW if there is a row in the table with the fqn but the node column contains null.
    */
   private Map loadNode(Fqn name)
   {
      boolean rowExists = false;
      Map oldNode = null;
      Connection con = null;
      PreparedStatement ps = null;
      ResultSet rs = null;
      try
      {
         if(log.isDebugEnabled())
         {
            log.debug("executing sql: " + selectNodeSql + " (" + name + ")");
         }

         con = cf.getConnection();
         ps = con.prepareStatement(selectNodeSql);
         ps.setString(1, name.toString());

         rs = ps.executeQuery();

         if(rs.next())
         {
            rowExists = true;
            InputStream is = rs.getBinaryStream(1);
            if(is != null && !rs.wasNull())
            {
               ObjectInputStream ois = null;
               try
               {
                  // deserialize result
                  ois = new ObjectInputStream(is);
                  Object marshalledNode = ois.readObject();

                  // de-marshall value if possible
                  if(marshalledNode instanceof MarshalledValue)
                  {
                     oldNode = (Map) ((MarshalledValue) marshalledNode).get();
                  }
                  else if(marshalledNode instanceof MarshalledObject)
                  {
                     oldNode = (Map) ((MarshalledObject) marshalledNode).get();
                  }
               }
               catch(IOException e)
               {
                  throw new SQLException("Unable to load to deserialize result: " + e);
               }
               catch(ClassNotFoundException e)
               {
                  throw new SQLException("Unable to load to deserialize result: " + e);
               }
               finally
               {
                  safeClose(ois);
               }
            }
         }
      }
      catch(SQLException e)
      {
         log.error("Failed to load node for fqn " + name + ": " + e.getMessage(), e);
         throw new IllegalStateException("Failed to load node for fqn " + name + ": " + e.getMessage());
      }
      finally
      {
         safeClose(rs);
         safeClose(ps);
         cf.close(con);
      }

      return oldNode == null ? (rowExists ? NULL_NODE_IN_ROW : null) : oldNode;
   }

   private static void safeClose(InputStream is)
   {
      if(is != null)
      {
         try
         {
            is.close();
         }
         catch(IOException e)
         {
            log.warn("Failed to close input stream: " + e.getMessage());
         }
      }
   }

   private static void safeClose(Connection con)
   {
      if(con != null)
      {
         try
         {
            con.close();
         }
         catch(SQLException e)
         {
            log.warn("Failed to close connection: " + e.getMessage());
         }
      }
   }

   private static void safeClose(Statement st)
   {
      if(st != null)
      {
         try
         {
            st.close();
         }
         catch(SQLException e)
         {
            log.warn("Failed to close statement: " + e.getMessage());
         }
      }
   }

   private static void safeClose(ResultSet rs)
   {
      if(rs != null)
      {
         try
         {
            rs.close();
         }
         catch(SQLException e)
         {
            log.warn("Failed to close result set: " + e.getMessage());
         }
      }
   }

   private static void loadDriver(String drv)
   {
      try
      {
         Class.forName(drv).newInstance();
      }
      catch(Exception e)
      {
         log.error("Failed to load driver " + drv, e);
         throw new IllegalStateException("Failed to load driver " + drv + ": " + e.getMessage());
      }
   }
   
   private static String getDriverName(Connection con)
   {
       if (con == null) return null;
      try{
         DatabaseMetaData dmd = con.getMetaData();
         return dmd.getDriverName().toUpperCase();         
      }
      catch(SQLException e)
      {
         // This should not happen. A J2EE compatiable JDBC driver is
         // required to fully support metadata.
         throw new IllegalStateException(
            "Error while getting the driver name " + ": " + e.getMessage());
      }       
   }

   private static String getRequiredProperty(Properties props, String name)
   {
      String value = props.getProperty(name);
      if(value == null)
      {
         throw new IllegalStateException("Missing required property: " + name);
      }
      return value;
   }

   private static boolean tableExists(String tableName, Connection con)
   {
      ResultSet rs = null;
      try
      {
         // (a j2ee spec compatible jdbc driver has to fully
         // implement the DatabaseMetaData)
         DatabaseMetaData dmd = con.getMetaData();
         String catalog = con.getCatalog();
         String schema = null;
         String quote = dmd.getIdentifierQuoteString();
         if(tableName.startsWith(quote))
         {
            if(!tableName.endsWith(quote))
            {
               throw new IllegalStateException("Mismatched quote in table name: " + tableName);
            }
            int quoteLength = quote.length();
            tableName = tableName.substring(quoteLength, tableName.length() - quoteLength);
            if(dmd.storesLowerCaseQuotedIdentifiers())
            {
               tableName = tableName.toLowerCase();
            }
            else if(dmd.storesUpperCaseQuotedIdentifiers())
            {
               tableName = tableName.toUpperCase();
            }
         }
         else
         {
            if(dmd.storesLowerCaseIdentifiers())
            {
               tableName = tableName.toLowerCase();
            }
            else if(dmd.storesUpperCaseIdentifiers())
            {
               tableName = tableName.toUpperCase();
            }
         }

         int dotIndex;
         if((dotIndex = tableName.indexOf('.')) != -1)
         {
            // Yank out schema name ...
            schema = tableName.substring(0, dotIndex);
            tableName = tableName.substring(dotIndex + 1);
         }

         rs = dmd.getTables(catalog, schema, tableName, null);
         return rs.next();
      }
      catch(SQLException e)
      {
         // This should not happen. A J2EE compatiable JDBC driver is
         // required fully support metadata.
         throw new IllegalStateException(
            "Error while checking if table aleady exists " + tableName + ": " + e.getMessage());
      }
      finally
      {
         safeClose(rs);
      }
   }

   // Inner

   private static final Map NULL_NODE_IN_ROW = new Map()
   {
      public int size()
      {
         throw new UnsupportedOperationException();
      }

      public void clear()
      {
         throw new UnsupportedOperationException();
      }

      public boolean isEmpty()
      {
         throw new UnsupportedOperationException();
      }

      public boolean containsKey(Object key)
      {
         throw new UnsupportedOperationException();
      }

      public boolean containsValue(Object value)
      {
         throw new UnsupportedOperationException();
      }

      public Collection values()
      {
         throw new UnsupportedOperationException();
      }

      public void putAll(Map t)
      {
         throw new UnsupportedOperationException();
      }

      public Set entrySet()
      {
         throw new UnsupportedOperationException();
      }

      public Set keySet()
      {
         throw new UnsupportedOperationException();
      }

      public Object get(Object key)
      {
         throw new UnsupportedOperationException();
      }

      public Object remove(Object key)
      {
         throw new UnsupportedOperationException();
      }

      public Object put(Object key, Object value)
      {
         throw new UnsupportedOperationException();
      }
   };

   interface ConnectionFactory
   {
      Connection getConnection() throws SQLException;

      Connection prepare(Object tx);

      void commit(Object tx);

      void rollback(Object tx);

      void close(Connection con);
   }

   private final class NonManagedConnectionFactory implements ConnectionFactory
   {
      private final String url;
      private final String usr;
      private final String pwd;

      public NonManagedConnectionFactory(String url, String usr, String pwd)
      {
         this.url = url;
         this.usr = usr;
         this.pwd = pwd;
      }

      public Connection prepare(Object tx)
      {
         Connection con = getConnection();
         try
         {
            if(con.getAutoCommit())
            {
               con.setAutoCommit(false);
            }
         }
         catch(Exception e)
         {
            log.error("Failed to set auto-commit: " + e.getMessage(), e);
            throw new IllegalStateException("Failed to set auto-commit: " + e.getMessage());
         }
         connection.set(con);
         return con;
      }

      public Connection getConnection()
      {
         Connection con = (Connection) connection.get();
         if(con == null)
         {
            try
            {
               con = DriverManager.getConnection(url, usr, pwd);
               connection.set(con);
            }
            catch(SQLException e)
            {
               log.error("Failed to get connection for url=" + url + ", user=" + usr + ", password=" + pwd, e);
               throw new IllegalStateException("Failed to get connection for url=" +
                  url +
                  ", user=" +
                  usr +
                  ", password=" +
                  pwd +
                  ": " +
                  e.getMessage());
            }
         }

         if(log.isTraceEnabled())
         {
            log.debug("using connection: " + con);
         }

         return con;
      }

      public void commit(Object tx)
      {
         Connection con = (Connection) connection.get();
         if(con == null)
         {
            throw new IllegalStateException("Failed to commit: thread is not associated with the connection!");
         }

         try
         {
            con.commit();
            if(log.isTraceEnabled())
            {
               log.trace("committed tx=" + tx + ", con=" + con);
            }
         }
         catch(SQLException e)
         {
            log.error("Failed to commit", e);
            throw new IllegalStateException("Failed to commit: " + e.getMessage());
         }
         finally
         {
            closeTxConnection(con);
         }
      }

      public void rollback(Object tx)
      {
         Connection con = (Connection) connection.get();
         if(con == null)
         {
            // todo: prepare was not called. why is rollback called?
            throw new IllegalStateException("Failed to rollback: thread is not associated with the connection!");
         }

         try
         {
            con.rollback();
            if(log.isTraceEnabled())
            {
               log.trace("rolledback tx=" + tx + ", con=" + con);
            }
         }
         catch(SQLException e)
         {
            log.error("Failed to rollback", e);
            throw new IllegalStateException("Failed to rollback: " + e.getMessage());
         }
         finally
         {
            closeTxConnection(con);
         }
      }

      public void close(Connection con)
      {
         if(con != null && con != connection.get())
         {
            try
            {
               con.close();
               if(log.isTraceEnabled())
               {
                  //log.trace("closed non tx connection: " + con);
               }
            }
            catch(SQLException e)
            {
               log.warn("Failed to close connection: " + e.getMessage());
            }
         }
      }

      private void closeTxConnection(Connection con)
      {
         safeClose(con);
         connection.set(null);
      }
   }

   private final class ManagedConnectionFactory
      implements ConnectionFactory
   {
      private final DataSource dataSource;

      public ManagedConnectionFactory(DataSource dataSource)
      {
         // Test that ds isn't null.  This wouldn't happen in the real
         // world as the JNDI lookup would fail, but this check here
         // allows a unit test using DummyContext
         if (dataSource == null)
            throw new IllegalArgumentException("dataSource cannot be null");
         
         this.dataSource = dataSource;
      }

      public Connection prepare(Object tx)
      {
         // we don't need to set autocommit to false beause the DataSource object
         // is almost always has a connection pool associated with it and distributed 
         // transaction participation turned on which means autocommit is off by default
         
         try
         {
            return getConnection();
         }
         catch(SQLException e)
         {
            log.error("Failed to get connection: " + e.getMessage(), e);
            throw new IllegalStateException("Failed to get connection: " + e.getMessage());
         }
      }

      public Connection getConnection()
         throws SQLException
      {
         return dataSource.getConnection();
      }

      public void commit(Object tx)
      {
      }

      public void rollback(Object tx)
      {
      }

      public void close(Connection con)
      {
         safeClose(con);
      }
   }
}
