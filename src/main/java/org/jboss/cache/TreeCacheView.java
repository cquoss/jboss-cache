/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache;


import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.swing.AbstractAction;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.mx.util.MBeanProxyExt;
import org.jboss.system.ServiceMBeanSupport;
import org.jgroups.View;


/**
 * Graphical view of a ReplicatedTree (using the MVC paradigm). An instance of this class needs to be given a
 * reference to the underlying model (ReplicatedTree) and needs to registers as a ReplicatedTreeListener. Changes
 * to the tree structure are propagated from the model to the view (via ReplicatedTreeListener), changes from the
 * GUI (e.g. by a user) are executed on the tree model (which will broadcast the changes to all replicas).<p>
 * The view itself caches only the nodes, but doesn't cache any of the data (HashMap) associated with it. When
 * data needs to be displayed, the underlying tree will be accessed directly.
 *
 * @version $Revision: 1574 $
 * @author<a href="mailto:bela@jboss.org">Bela Ban</a> March 27 2003
 * @jmx.mbean extends="org.jboss.system.ServiceMBean"
 */
public class TreeCacheView extends ServiceMBeanSupport implements TreeCacheViewMBean {
   /**
    * Reference to the TreeCache MBean (the model for this view)
    */
   ObjectName cache_service=null;
   TreeCacheGui gui=null;
   TreeCacheMBean cache;


   public TreeCacheView() throws Exception {
      super();
      init(null);
   }

   public TreeCacheView(String cache_service) throws Exception {
      init(cache_service);
   }


   /**
    * @throws Exception
    * @jmx.managed-operation
    */
   public void create() throws Exception {
      super.create();
   }

   /**
    * @throws Exception
    * @jmx.managed-operation
    */
   public void start() throws Exception {
      super.start();
      if(gui == null) {
         log.info("start(): creating the GUI");
         gui=new TreeCacheGui(cache);
      }
   }

   /**
    * @jmx.managed-operation
    */
   public void stop() {
      super.stop();
      if(gui != null) {
         log.info("stop(): disposing the GUI");
         gui.dispose();
         gui=null;
      }
   }

   /**
    * @jmx.managed-operation
    */
   public void destroy() {
      super.destroy();
   }

   /**
    * @return
    * @jmx.managed-attribute
    */
   public String getCacheService() {
      return cache_service != null ? cache_service.toString() : null;
   }

   /**
    * @param cache_service
    * @throws Exception
    * @jmx.managed-attribute
    */
   public void setCacheService(String cache_service) throws Exception {
      init(cache_service);
   }


   void init(String cache_service) throws Exception {
      MBeanServer srv=null;

      if(cache_service != null)
         this.cache_service=new ObjectName(cache_service);
      else
         return;

      // is this the right way to get hold of the JBoss MBeanServer ?
      List servers=MBeanServerFactory.findMBeanServer(null);
      if(servers == null || servers.size() == 0)
         throw new Exception("TreeCacheView.init(): no MBeanServers found");
      srv=(MBeanServer)servers.get(0);
      log.info("init(): found MBeanServer " + srv);
      cache=(TreeCacheMBean)MBeanProxyExt.create(TreeCacheMBean.class, cache_service, srv);
   }


   void populateTree(String dir) throws Exception {
      File file=new File(dir);

      if(!file.exists()) return;

      put(dir, null);

      if(file.isDirectory()) {
         String[] children=file.list();
         if(children != null && children.length > 0) {
            for(int i=0; i < children.length; i++)
               populateTree(dir + "/" + children[i]);
         }
      }
   }

   void put(String fqn, Map m) {
      try {
         cache.put(fqn, m);
      }
      catch(Throwable t) {
         log.error("TreeCacheView.put(): " + t);
      }
   }


   public static void main(String args[]) {
      TreeCache tree=null;
      TreeCacheView demo;
      String start_directory=null;
      String mbean_name="jboss.cache:service=TreeCache";
      String props=getDefaultProps();
      MBeanServer srv;
      Log log;
      boolean use_queue=false;
      int queue_interval=5000;
      int queue_max_elements=100;


      for(int i=0; i < args.length; i++) {
         if(args[i].equals("-mbean_name")) {
            mbean_name=args[++i];
            continue;
         }
         if(args[i].equals("-props")) {
            props=args[++i];
            continue;
         }
         if(args[i].equals("-start_directory")) {
            start_directory=args[++i];
            continue;
         }
         if(args[i].equals("-use_queue")) {
            use_queue=true;
            continue;
         }
         if(args[i].equals("-queue_interval")) {
            queue_interval=Integer.parseInt(args[++i]);
            use_queue=true;
            continue;
         }
         if(args[i].equals("-queue_max_elements")) {
            queue_max_elements=Integer.parseInt(args[++i]);
            use_queue=true;
            continue;
         }
         help();
         return;
      }

      try {
         log=LogFactory.getLog(TreeCache.class);
         srv=MBeanServerFactory.createMBeanServer();

//            String  FACTORY="org.jboss.cache.transaction.DummyContextFactory";
//            System.setProperty(Context.INITIAL_CONTEXT_FACTORY, FACTORY);
//
//            DummyTransactionManager.getInstance();


         tree=new TreeCache();
         tree.setClusterName("TreeCacheGroup");
         tree.setClusterProperties(props);
         tree.setInitialStateRetrievalTimeout(10000);
         tree.setCacheMode(TreeCache.REPL_ASYNC);

         if(use_queue) {
            tree.setUseReplQueue(true);
            tree.setReplQueueInterval(queue_interval);
            tree.setReplQueueMaxElements(queue_max_elements);
         }

         tree.addTreeCacheListener(new MyListener());

         log.info("registering the tree as " + mbean_name);
         srv.registerMBean(tree, new ObjectName(mbean_name));

         tree.start();

         Runtime.getRuntime().addShutdownHook(new ShutdownThread(tree));

//            tree.put("/a/b/c", null);
//            tree.put("/a/b/c1", null);
//            tree.put("/a/b/c2", null);
//            tree.put("/a/b1/chat", null);
//            tree.put("/a/b1/chat2", null);
//            tree.put("/a/b1/chat5", null);

         demo=new TreeCacheView(mbean_name);
         demo.create();
         demo.start();
         if(start_directory != null && start_directory.length() > 0) {
            demo.populateTree(start_directory);
         }
      }
      catch(Exception ex) {
         ex.printStackTrace();
      }
   }

   static class ShutdownThread extends Thread {
      TreeCache tree=null;

      ShutdownThread(TreeCache tree) {
         this.tree=tree;
      }

      public void run() {
         tree.stop();
      }
   }

   private static String getDefaultProps() {
      return
              "UDP(ip_mcast=true;ip_ttl=64;loopback=false;mcast_addr=228.1.2.3;" +
              "mcast_port=45566;mcast_recv_buf_size=80000;mcast_send_buf_size=150000;" +
              "ucast_recv_buf_size=80000;ucast_send_buf_size=150000):" +
              "PING(down_thread=true;num_initial_members=3;timeout=2000;up_thread=true):" +
              "MERGE2(max_interval=20000;min_interval=10000):" +
              "FD(down_thread=true;shun=true;up_thread=true):" +
              "VERIFY_SUSPECT(down_thread=true;timeout=1500;up_thread=true):" +
              "pbcast.NAKACK(down_thread=true;gc_lag=50;retransmit_timeout=600,1200,2400,4800;" +
              "up_thread=true):" +
              "pbcast.STABLE(desired_avg_gossip=20000;down_thread=true;up_thread=true):" +
              "UNICAST(down_thread=true;min_threshold=10;timeout=600,1200,2400;window_size=100):" +
              "FRAG(down_thread=true;frag_size=8192;up_thread=true):" +
              "pbcast.GMS(join_retry_timeout=2000;join_timeout=5000;print_local_addr=true;shun=true):" +
              "pbcast.STATE_TRANSFER(down_thread=true;up_thread=true)";
   }


   static void help() {
      System.out.println("TreeCacheView [-help] " +
              "[-mbean_name <name of TreeCache MBean>] " +
              "[-start_directory <dirname>] [-props <props>] " +
              "[-use_queue <true/false>] [-queue_interval <ms>] " +
              "[-queue_max_elements <num>]");
   }


   public static class MyListener extends AbstractTreeCacheListener {}


}


class TreeCacheGui extends JFrame implements WindowListener, TreeCacheListener,
        TreeSelectionListener, TableModelListener {
   private static final long serialVersionUID = 8576324868563647538L;
   TreeCacheMBean cache;
   DefaultTreeModel tree_model=null;
   Log log=LogFactory.getLog(getClass());
   JTree jtree=null;
   DefaultTableModel table_model=new DefaultTableModel();
   JTable table=new JTable(table_model);
   MyNode root=new MyNode(SEP);
   String selected_node=null;
   JPanel tablePanel=null;
   JMenu operationsMenu=null;
   JPopupMenu operationsPopup=null;
   JMenuBar menubar=null;
   static final String SEP=Fqn.SEPARATOR;
   private static final int KEY_COL_WIDTH=20;
   private static final int VAL_COL_WIDTH=300;


   public TreeCacheGui(TreeCacheMBean cache) throws Exception {
      this.cache=cache;

      //server.invoke(cache_service, "addTreeCacheListener",
      //            new Object[]{this},
      //          new String[]{TreeCacheListener.class.getName()});
      cache.addTreeCacheListener(this);
      addNotify();
      setTitle("TreeCacheGui: mbr=" + getLocalAddress());

      tree_model=new DefaultTreeModel(root);
      jtree=new JTree(tree_model);
      jtree.setDoubleBuffered(true);
      jtree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);

      JScrollPane scroll_pane=new JScrollPane(jtree);

      populateTree();

      getContentPane().add(scroll_pane, BorderLayout.CENTER);
      addWindowListener(this);

      table_model.setColumnIdentifiers(new String[]{"Name", "Value"});
      table_model.addTableModelListener(this);

      setTableColumnWidths();

      tablePanel=new JPanel();
      tablePanel.setLayout(new BorderLayout());
      tablePanel.add(table.getTableHeader(), BorderLayout.NORTH);
      tablePanel.add(table, BorderLayout.CENTER);

      getContentPane().add(tablePanel, BorderLayout.SOUTH);

      jtree.addTreeSelectionListener(this);//REVISIT

      MouseListener ml=new MouseAdapter() {
         public void mouseClicked(MouseEvent e) {
            int selRow=jtree.getRowForLocation(e.getX(), e.getY());
            TreePath selPath=jtree.getPathForLocation(e.getX(), e.getY());
            if(selRow != -1) {
               selected_node=makeFQN(selPath.getPath());
               jtree.setSelectionPath(selPath);

               if(e.getModifiers() == java.awt.event.InputEvent.BUTTON3_MASK) {
                  operationsPopup.show(e.getComponent(),
                          e.getX(), e.getY());
               }
            }
         }
      };

      jtree.addMouseListener(ml);

      createMenus();
      setLocation(50, 50);
      setSize(getInsets().left + getInsets().right + 485,
              getInsets().top + getInsets().bottom + 367);

      init();
      setVisible(true);
   }

   void setSystemExit(boolean flag) {
   }

   public void windowClosed(WindowEvent event) {
   }

   public void windowDeiconified(WindowEvent event) {
   }

   public void windowIconified(WindowEvent event) {
   }

   public void windowActivated(WindowEvent event) {
   }

   public void windowDeactivated(WindowEvent event) {
   }

   public void windowOpened(WindowEvent event) {
   }

   public void windowClosing(WindowEvent event) {
      dispose();
   }


   public void tableChanged(TableModelEvent evt) {
      int row, col;
      String key, val;

      if(evt.getType() == TableModelEvent.UPDATE) {
         row=evt.getFirstRow();
         col=evt.getColumn();
         if(col == 0) {  // set()
            key=(String)table_model.getValueAt(row, col);
            val=(String)table_model.getValueAt(row, col + 1);
            if(key != null && val != null) {
               // tree.put(selected_node, key, val);

//                        server.invoke(cache_service, "put",
//                                      new Object[]{selected_node, key, val},
//                                      new String[]{String.class.getName(),
//                                      String.class.getName(), Object.class.getName()});
               try {
                  cache.put(selected_node, key, val);
               }
               catch(Exception e) {
                  e.printStackTrace();
               }

            }
         }
         else {          // add()
            key=(String)table_model.getValueAt(row, col - 1);
            val=(String)table.getValueAt(row, col);
            if(key != null && val != null) {
               put(selected_node, key, val);
            }
         }
      }
   }


   public void valueChanged(TreeSelectionEvent evt) {
      TreePath path=evt.getPath();
      String fqn=SEP;
      String component_name;
      Map data=null;

      for(int i=0; i < path.getPathCount(); i++) {
         component_name=((MyNode)path.getPathComponent(i)).name;
         if(component_name.equals(SEP))
            continue;
         if(fqn.equals(SEP))
            fqn+=component_name;
         else
            fqn=fqn + SEP + component_name;
      }
      data=getData(fqn);
      if(data != null) {
         getContentPane().add(tablePanel, BorderLayout.SOUTH);
         populateTable(data);
         validate();
      }
      else {
         clearTable();
         getContentPane().remove(tablePanel);
         validate();
      }
   }



   /* ------------------ ReplicatedTree.ReplicatedTreeListener interface ------------ */

   public void nodeCreated(Fqn fqn) {
      MyNode n, p;

      n=root.add(fqn.toString());
      if(n != null) {
         p=(MyNode)n.getParent();
         tree_model.reload(p);
         jtree.scrollPathToVisible(new TreePath(n.getPath()));
      }
   }

   public void nodeRemoved(Fqn fqn) {
      MyNode n;
      TreeNode par;

      n=root.findNode(fqn.toString());
      if(n != null) {
         n.removeAllChildren();
         par=n.getParent();
         n.removeFromParent();
         tree_model.reload(par);
      }
   }

   public void nodeLoaded(Fqn fqn) {
      nodeCreated(fqn);
   }
   
   public void nodeEvicted(Fqn fqn) {
      nodeRemoved(fqn);
   }

   public void nodeModified(Fqn fqn) {
      // Map data;
      //data=getData(fqn);
      //populateTable(data); REVISIT
      /*
        poulateTable is the current table being shown is the info of the node. that is modified.
      */
   }

   public void nodeVisited(Fqn fqn)
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   public void cacheStarted(TreeCache cache)
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   public void cacheStopped(TreeCache cache)
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   public void viewChange(final View new_view) {
      new Thread() {
         public void run() {
            Vector mbrship;
            if(new_view != null && (mbrship=new_view.getMembers()) != null) {
               _put(SEP, "members", mbrship);
               _put(SEP, "coordinator", mbrship.firstElement());
            }
         }
      }.start();
   }




   /* ---------------- End of ReplicatedTree.ReplicatedTreeListener interface -------- */

   /*----------------- Runnable implementation to make View change calles in AWT Thread ---*/

   public void run() {

   }



   /* ----------------------------- Private Methods ---------------------------------- */

   /**
    * Fetches all data from underlying tree model and display it graphically
    */
   void init() {
      Vector mbrship=null;

      addGuiNode(SEP);

      mbrship=getMembers() != null ? (Vector)getMembers().clone() : null;
      if(mbrship != null && mbrship.size() > 0) {
         _put(SEP, "members", mbrship);
         _put(SEP, "coordinator", mbrship.firstElement());
      }
   }


   /**
    * Fetches all data from underlying tree model and display it graphically
    */
   private void populateTree() {
      addGuiNode(SEP);
   }


   /**
    * Recursively adds GUI nodes starting from fqn
    */
   void addGuiNode(String fqn) {
      Set children;
      String child_name;

      if(fqn == null) return;

      // 1 . Add myself
      root.add(fqn);

      // 2. Then add my children
      children=getChildrenNames(fqn);
      if(children != null) {
         for(Iterator it=children.iterator(); it.hasNext();) {
            child_name=it.next().toString();
            addGuiNode(fqn + SEP + child_name);
         }
      }
   }


   String makeFQN(Object[] path) {
      StringBuffer sb=new StringBuffer("");
      String tmp_name;

      if(path == null) return null;
      for(int i=0; i < path.length; i++) {
         tmp_name=((MyNode)path[i]).name;
         if(tmp_name.equals(SEP))
            continue;
         else
            sb.append(SEP + tmp_name);
      }
      tmp_name=sb.toString();
      if(tmp_name.length() == 0)
         return SEP;
      else
         return tmp_name;
   }

   void clearTable() {
      int num_rows=table.getRowCount();

      if(num_rows > 0) {
         for(int i=0; i < num_rows; i++)
            table_model.removeRow(0);
         table_model.fireTableRowsDeleted(0, num_rows - 1);
         repaint();
      }
   }


   void populateTable(Map data) {
      String key, strval="<null>";
      Object val;
      int num_rows=0;
      Map.Entry entry;

      if(data == null) return;
      num_rows=data.size();
      clearTable();

      if(num_rows > 0) {
         for(Iterator it=data.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=(String)entry.getKey();
            val=entry.getValue();
            if(val != null) strval=val.toString();
            table_model.addRow(new Object[]{key, strval});
         }
         table_model.fireTableRowsInserted(0, num_rows - 1);
         validate();
      }
   }

   private void setTableColumnWidths() {
      table.sizeColumnsToFit(JTable.AUTO_RESIZE_NEXT_COLUMN);
      TableColumn column=null;
      column=table.getColumnModel().getColumn(0);
      column.setMinWidth(KEY_COL_WIDTH);
      column.setPreferredWidth(KEY_COL_WIDTH);
      column=table.getColumnModel().getColumn(1);
      column.setPreferredWidth(VAL_COL_WIDTH);
   }

   private void createMenus() {
      menubar=new JMenuBar();
      operationsMenu=new JMenu("Operations");
      AddNodeAction addNode=new AddNodeAction();
      addNode.putValue(AbstractAction.NAME, "Add to this node");
      RemoveNodeAction removeNode=new RemoveNodeAction();
      removeNode.putValue(AbstractAction.NAME, "Remove this node");
      AddModifyDataForNodeAction addModAction=new AddModifyDataForNodeAction();
      addModAction.putValue(AbstractAction.NAME, "Add/Modify data");
      PrintLockInfoAction print_locks=new PrintLockInfoAction();
      print_locks.putValue(AbstractAction.NAME, "Print lock information (stdout)");
      ReleaseAllLocksAction release_locks=new ReleaseAllLocksAction();
      release_locks.putValue(AbstractAction.NAME, "Release all locks");
      ExitAction exitAction=new ExitAction();
      exitAction.putValue(AbstractAction.NAME, "Exit");
      operationsMenu.add(addNode);
      operationsMenu.add(removeNode);
      operationsMenu.add(addModAction);
      operationsMenu.add(print_locks);
      operationsMenu.add(release_locks);
      operationsMenu.add(exitAction);
      menubar.add(operationsMenu);
      setJMenuBar(menubar);

      operationsPopup=new JPopupMenu();
      operationsPopup.add(addNode);
      operationsPopup.add(removeNode);
      operationsPopup.add(addModAction);
   }

   Object getLocalAddress() {
      try {
         return cache.getLocalAddress();
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.getLocalAddress(): " + t);
         return null;
      }
   }

   Map getData(String fqn) {
      Map data;
      Set keys;
      String key;
      Object value;

      if(fqn == null) return null;
      keys=getKeys(fqn);
      if(keys == null) return null;
      data=new HashMap();
      for(Iterator it=keys.iterator(); it.hasNext();) {
         key=(String)it.next();
         value=get(fqn, key);
         if(value != null)
            data.put(key, value);
      }
      return data;
   }


   void put(String fqn, Map m) {
      try {
         cache.put(fqn, m);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.put(): " + t);
      }
   }


   void put(String fqn, String key, Object value) {
      try {
         cache.put(fqn, key, value);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.put(): " + t);
      }
   }

   void _put(String fqn, String key, Object value) {
      try {
         cache._put(null, fqn, key, value, false);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui._put(): " + t);
      }
   }

   Set getKeys(String fqn) {
      try {
         return cache.getKeys(fqn);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.getKeys(): " + t);
         return null;
      }
   }

   Object get(String fqn, String key) {
      try {
         return cache.get(fqn, key);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.get(): " + t);
         return null;
      }
   }

   Set getChildrenNames(String fqn) {
      try {
         return cache.getChildrenNames(fqn);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.getChildrenNames(): " + t);
         return null;
      }
   }

   Vector getMembers() {
      try {
         return cache.getMembers();
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.getMembers(): " + t);
         return null;
      }
   }


   /* -------------------------- End of Private Methods ------------------------------ */

   /*----------------------- Actions ---------------------------*/
   class ExitAction extends AbstractAction {
      private static final long serialVersionUID = 8895044368299888998L;
      public void actionPerformed(ActionEvent e) {
         dispose();
      }
   }

   class AddNodeAction extends AbstractAction {
      private static final long serialVersionUID = 5568518714172267901L;
      public void actionPerformed(ActionEvent e) {
         JTextField fqnTextField=new JTextField();
         if(selected_node != null)
            fqnTextField.setText(selected_node);
         Object[] information={"Enter fully qualified name",
                               fqnTextField};
         final String btnString1="OK";
         final String btnString2="Cancel";
         Object[] options={btnString1, btnString2};
         int userChoice=JOptionPane.showOptionDialog(null,
                 information,
                 "Add DataNode",
                 JOptionPane.YES_NO_OPTION,
                 JOptionPane.PLAIN_MESSAGE,
                 null,
                 options,
                 options[0]);
         if(userChoice == 0) {
            String userInput=fqnTextField.getText();
            put(userInput, null);
         }
      }
   }


   class PrintLockInfoAction extends AbstractAction {
      private static final long serialVersionUID = 5577441016277949170L;
      public void actionPerformed(ActionEvent e) {
         System.out.println("\n*** lock information ****\n" + cache.printLockInfo());
      }
   }

   class ReleaseAllLocksAction extends AbstractAction {
      private static final long serialVersionUID = 3796901116451916116L;
      public void actionPerformed(ActionEvent e) {
         cache.releaseAllLocks("/");
      }
   }

   class RemoveNodeAction extends AbstractAction {
      private static final long serialVersionUID = 8985697625953238855L;
      public void actionPerformed(ActionEvent e) {
         //remove(selected_node);
         try {
            //server.invoke(cache_service, "remove",
            //            new Object[]{selected_node},
            //          new String[]{STRING});
            cache.remove(selected_node);
         }
         catch(Throwable t) {
            log.error("RemoveNodeAction.actionPerformed(): " + t);
         }
      }
   }

   class AddModifyDataForNodeAction extends AbstractAction {
      private static final long serialVersionUID = 3593129982953807846L;
      public void actionPerformed(ActionEvent e) {
         Map data=getData(selected_node);
         if(data != null) {
         }
         else {
            clearTable();
            data=new HashMap();
            data.put("Add Key", "Add Value");

         }
         populateTable(data);
         getContentPane().add(tablePanel, BorderLayout.SOUTH);
         validate();

      }
   }


   class MyNode extends DefaultMutableTreeNode {
      private static final long serialVersionUID = 1578599138419577069L;
      String name="<unnamed>";


      MyNode(String name) {
         this.name=name;
      }


      /**
       * Adds a new node to the view. Intermediary nodes will be created if they don't yet exist.
       * Returns the first node that was created or null if node already existed
       */
      public MyNode add(String fqn) {
         MyNode curr, n, ret=null;
         StringTokenizer tok;
         String child_name;

         if(fqn == null) return null;
         curr=this;
         tok=new StringTokenizer(fqn, TreeCacheGui.SEP);

         while(tok.hasMoreTokens()) {
            child_name=tok.nextToken();
            n=curr.findChild(child_name);
            if(n == null) {
               n=new MyNode(child_name);
               if(ret == null) ret=n;
               curr.add(n);
            }
            curr=n;
         }
         return ret;
      }


      /**
       * Removes a node from the view. Child nodes will be removed as well
       */
      public void remove(String fqn) {
         removeFromParent();
      }


      MyNode findNode(String fqn) {
         MyNode curr, n;
         StringTokenizer tok;
         String child_name;

         if(fqn == null) return null;
         curr=this;
         tok=new StringTokenizer(fqn, TreeCacheGui.SEP);

         while(tok.hasMoreTokens()) {
            child_name=tok.nextToken();
            n=curr.findChild(child_name);
            if(n == null)
               return null;
            curr=n;
         }
         return curr;
      }


      MyNode findChild(String relative_name) {
         MyNode child;

         if(relative_name == null || getChildCount() == 0)
            return null;
         for(int i=0; i < getChildCount(); i++) {
            child=(MyNode)getChildAt(i);
            if(child.name == null) {
               continue;
            }

            if(child.name.equals(relative_name))
               return child;
         }
         return null;
      }


      String print(int indent) {
         StringBuffer sb=new StringBuffer();

         for(int i=0; i < indent; i++)
            sb.append(" ");
         if(!isRoot()) {
            if(name == null)
               sb.append("/<unnamed>");
            else {
               sb.append(TreeCacheGui.SEP + name);
            }
         }
         sb.append("\n");
         if(getChildCount() > 0) {
            if(isRoot())
               indent=0;
            else
               indent+=4;
            for(int i=0; i < getChildCount(); i++)
               sb.append(((MyNode)getChildAt(i)).print(indent));
         }
         return sb.toString();
      }


      public String toString() {
         return name;
      }

   }




}












