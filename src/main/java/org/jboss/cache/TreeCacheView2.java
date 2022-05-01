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
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.AbstractAction;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
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
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.View;

import bsh.Interpreter;
import bsh.util.JConsole;


/**
 * Graphical view of a ReplicatedTree (using the MVC paradigm). An instance of this class needs to be given a
 * reference to the underlying model (ReplicatedTree) and needs to registers as a ReplicatedTreeListener. Changes
 * to the tree structure are propagated from the model to the view (via ReplicatedTreeListener), changes from the
 * GUI (e.g. by a user) are executed on the tree model (which will broadcast the changes to all replicas).<p>
 * The view itself caches only the nodes, but doesn't cache any of the data (HashMap) associated with it. When
 * data needs to be displayed, the underlying tree will be accessed directly.
 *
 * @version $Revision: 2164 $
 */
public class TreeCacheView2 {
   static TreeCacheGui2 gui_=null;
   static boolean useConsole = false;
   TreeCache cache_=null;
   static Log log_=LogFactory.getLog(TreeCacheView2.class.getName());

   public TreeCacheView2(TreeCache cache) throws Exception {
      this.cache_=cache;
   }

   public static void setCache(TreeCache cache)
   {
	   gui_.setCache(cache);
   }
   public void start() throws Exception {
      if(gui_ == null) {
         log_.info("start(): creating the GUI");
         gui_=new TreeCacheGui2(cache_);
      }
   }

   public void stop() {
      if(gui_ != null) {
         log_.info("stop(): disposing the GUI");
         gui_.stopGui();
         gui_=null;
      }
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
         cache_.put(fqn, m);
      }
      catch(Throwable t) {
         log_.error("TreeCacheView2.put(): " + t);
      }
   }

   public static void main(String args[]) {
		TreeCache tree = null;
		TreeCacheView2 demo;
		String start_directory = null;
		String resource = "META-INF/replSync-service.xml";

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-console")) {
				useConsole = true;
				continue;
			}
			if (args[i].equals("-config")) {
				resource = args[++i];
				continue;
			}
			help();
			return;
		}

		try {
			if (useConsole) {
				demo = new TreeCacheView2(null);
				demo.start();
			} else {
				tree = new TreeCache();
				PropertyConfigurator config = new PropertyConfigurator();
				config.configure(tree, resource);

				tree.addTreeCacheListener(new TreeCacheView.MyListener());
				tree.start();
				demo = new TreeCacheView2(tree);
				demo.start();
				if (start_directory != null && start_directory.length() > 0) {
					demo.populateTree(start_directory);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
   static void help() {
      System.out.println("TreeCacheView [-help] " +
              "[-mbean_name <name of TreeCache MBean>] " +
              "[-start_directory <dirname>] [-props <props>] " +
              "[-use_queue <true/false>] [-queue_interval <ms>] [-console]" +
              "[-queue_max_elements <num>]");
   }

}

class ShutdownThread extends Thread {
    TreeCache tree=null;

    ShutdownThread(TreeCache tree) {
       this.tree=tree;
    }

    public void run() {
       tree.stopService();
    }
 }

class TreeCacheGui2 extends JFrame implements WindowListener, TreeCacheListener,
        TreeSelectionListener, TableModelListener {
   private static final long serialVersionUID = -1242167331988194987L;

   TreeCache cache_;
   DefaultTreeModel tree_model=null;
   Log log=LogFactory.getLog(getClass());
   JTree jtree=null;
   DefaultTableModel table_model=new DefaultTableModel();
   JTable table=new JTable(table_model);
   MyNode root=new MyNode(SEP);
   String props=null;
   String selected_node=null;
   JPanel tablePanel=null;
   JMenu operationsMenu=null;
   JPopupMenu operationsPopup=null;
   JMenuBar menubar=null;
   boolean use_system_exit=false;
   static String SEP=Fqn.SEPARATOR;
   private static final int KEY_COL_WIDTH=20;
   private static final int VAL_COL_WIDTH=300;
   final String STRING=String.class.getName();
   final String MAP=Map.class.getName();
   final String OBJECT=Object.class.getName();
   TransactionManager tx_mgr=null;
   Transaction tx=null;
   JPanel mainPanel;


   public TreeCacheGui2(TreeCache cache) throws Exception {
		addNotify();

		tree_model = new DefaultTreeModel(root);
		jtree = new JTree(tree_model);
		jtree.setDoubleBuffered(true);
		jtree.getSelectionModel().setSelectionMode(
				TreeSelectionModel.SINGLE_TREE_SELECTION);

		JScrollPane scroll_pane = new JScrollPane(jtree);
		mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		mainPanel.add(scroll_pane, BorderLayout.CENTER);

		addWindowListener(this);

		table_model.setColumnIdentifiers(new String[] { "Name", "Value" });
		table_model.addTableModelListener(this);

		setTableColumnWidths();

		tablePanel = new JPanel();
		tablePanel.setLayout(new BorderLayout());
		tablePanel.add(table.getTableHeader(), BorderLayout.NORTH);
		tablePanel.add(table, BorderLayout.CENTER);

		mainPanel.add(tablePanel, BorderLayout.SOUTH);
		JSplitPane contentPanel=null;
		if (TreeCacheView2.useConsole) {
			JConsole bshConsole = new JConsole();
			Interpreter interpreter = new Interpreter(bshConsole);
			interpreter.getNameSpace().importCommands("org.jboss.cache.util");
			interpreter.setShowResults(!interpreter.getShowResults()); // show();
			//System.setIn(bshConsole.getInputStream());
			System.setOut(bshConsole.getOut());
			System.setErr(bshConsole.getErr());
			Thread t = new Thread(interpreter);
			t.start();

			contentPanel = new JSplitPane(JSplitPane.VERTICAL_SPLIT,mainPanel, bshConsole);
			getContentPane().add(contentPanel);
		} else {
			getContentPane().add(mainPanel);
		}
		jtree.addTreeSelectionListener(this);// REVISIT

		MouseListener ml = new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
				int selRow = jtree.getRowForLocation(e.getX(), e.getY());
				TreePath selPath = jtree.getPathForLocation(e.getX(), e.getY());
				if (selRow != -1) {
					selected_node = makeFQN(selPath.getPath());
					jtree.setSelectionPath(selPath);

					if (e.getModifiers() == java.awt.event.InputEvent.BUTTON3_MASK) {
						operationsPopup.show(e.getComponent(), e.getX(), e
								.getY());
					}
				}
			}
		};

		jtree.addMouseListener(ml);

		createMenus();
		setLocation(50, 50);
		setSize(getInsets().left + getInsets().right + 485, getInsets().top
				+ getInsets().bottom + 367);

		init();
		setCache(cache);
		setVisible(true);

		if(TreeCacheView2.useConsole)
		{
			//has to be called after setVisible() otherwise no effect
			contentPanel.setDividerLocation(0.65);
		}
	}

   public void setCache(TreeCache tree)
   {
	   cache_ = tree;
	   if(cache_!= null)
	   {
		   Runtime.getRuntime().addShutdownHook(new ShutdownThread(tree));
		   cache_.addTreeCacheListener(this);
		   setTitle("TreeCacheGui2: mbr=" + getLocalAddress());
		   tx_mgr=cache_.getTransactionManager();
		   populateTree();
	   }
	   else
	   {
		   setTitle("Cache undefined");
		   if(tree_model!=null)
		   {
			   root = new MyNode(SEP);
			   tree_model.setRoot(root);
			   tree_model.reload();

			}
		   if(table_model!=null)
		   {
			   clearTable();
		   }
	   }
   }

   void setSystemExit(boolean flag) {
      use_system_exit=flag;
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
      stopGui();
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

               try {
                  cache_.put(selected_node, key, val);
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

      data=getData(Fqn.fromString(fqn));
      if(data != null) {
         mainPanel.add(tablePanel, BorderLayout.SOUTH);
         populateTable(data);
         validate();
      }
      else {
         clearTable();
         mainPanel.remove(tablePanel);
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
      Map data;
      data=getData(fqn);
      /*
        poulateTable is the current table being shown is the info of the node. that is modified.
      */
      populateTable(data);
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

      // addGuiNode(SEP);
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
      LoadAction load_action=new LoadAction();
      load_action.putValue(AbstractAction.NAME, "Load from the CacheLoader");
      RemoveNodeAction removeNode=new RemoveNodeAction();
      removeNode.putValue(AbstractAction.NAME, "Remove this node");
      EvictAction evict_action=new EvictAction();
      evict_action.putValue(AbstractAction.NAME, "Evict from the Cache");
      AddModifyDataForNodeAction addModAction=new AddModifyDataForNodeAction();
      addModAction.putValue(AbstractAction.NAME, "Add/Modify data");
      PrintLockInfoAction print_locks=new PrintLockInfoAction();
      print_locks.putValue(AbstractAction.NAME, "Print lock information (stdout)");
      ReleaseAllLocksAction release_locks=new ReleaseAllLocksAction();
      release_locks.putValue(AbstractAction.NAME, "Release all locks");
      ExitAction exitAction=new ExitAction();
      exitAction.putValue(AbstractAction.NAME, "Exit");
      StartTransaction start_tx=new StartTransaction();
      start_tx.putValue(AbstractAction.NAME, "Start TX");
      CommitTransaction commit_tx=new CommitTransaction();
      commit_tx.putValue(AbstractAction.NAME, "Commit TX");
      RollbackTransaction rollback_tx=new RollbackTransaction();
      rollback_tx.putValue(AbstractAction.NAME, "Rollback TX");
      operationsMenu.add(addNode);
      operationsMenu.add(load_action);
      operationsMenu.add(removeNode);
      operationsMenu.add(evict_action);
      operationsMenu.add(addModAction);
      operationsMenu.add(print_locks);
      operationsMenu.add(release_locks);
      operationsMenu.add(start_tx);
      operationsMenu.add(commit_tx);
      operationsMenu.add(rollback_tx);
      operationsMenu.add(exitAction);
      menubar.add(operationsMenu);
      setJMenuBar(menubar);

      operationsPopup=new JPopupMenu();
      operationsPopup.add(addNode);
      operationsPopup.add(load_action);
      operationsPopup.add(evict_action);
      operationsPopup.add(removeNode);
      operationsPopup.add(addModAction);
   }

   Object getLocalAddress() {
      try {
         return cache_.getLocalAddress();
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.getLocalAddress(): " + t);
         return null;
      }
   }

   Map getData(Fqn fqn) {
      /*
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
      */
      // Retreive it directly from internal cache bypassing the lock such there is no chance of deadlock.
      return cache_._getData(fqn);
   }


   void load(String fqn) {
      try {
         cache_.load(fqn);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.load(): " + t);
      }
   }

   void evict(String fqn) {
      try {
         cache_.evict(Fqn.fromString(fqn));
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.evict(): " + t);
      }
   }

   void put(String fqn, Map m) {
      try {
         cache_.put(fqn, m);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.put(): " + t);
      }
   }

   void _put(String fqn, Map m) {
      try {
         cache_._put(null, fqn, m, false);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2._put(): " + t);
      }
   }

   void put(String fqn, String key, Object value) {
      try {
         cache_.put(fqn, key, value);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.put(): " + t);
      }
   }

   void _put(String fqn, String key, Object value) {
      try {
         cache_._put(null, fqn, key, value, false);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2._put(): " + t);
      }
   }

   Set getKeys(String fqn) {
      try {
         return cache_.getKeys(fqn);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.getKeys(): " + t);
         return null;
      }
   }

   Object get(String fqn, String key) {
      try {
         return cache_.get(fqn, key);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.get(): " + t);
         return null;
      }
   }

   Set getChildrenNames(String fqn) {
      try {
         return cache_.getChildrenNames(fqn);
      }
      catch(Throwable t) {
         log.error("TreeCacheGui2.getChildrenNames(): " + t);
         return null;
      }
   }

   Vector getMembers() {
      try {
         return cache_.getMembers();
      }
      catch(Throwable t) {
         log.error("TreeCacheGui.getMembers(): " + t);
         return null;
      }
   }


   /* -------------------------- End of Private Methods ------------------------------ */

   /*----------------------- Actions ---------------------------*/
   class ExitAction extends AbstractAction {
      private static final long serialVersionUID = -5364163916172148038L;
      public void actionPerformed(ActionEvent e) {
         stopGui();
      }
   }


   void stopGui() {
      if(cache_ != null) {
         try {
            cache_.stopService();
            cache_.destroyService();
            cache_=null;
         }
         catch(Throwable t) {
            t.printStackTrace();
         }
      }
      dispose();
      System.exit(0);
   }

   class AddNodeAction extends AbstractAction {
      private static final long serialVersionUID = 7084928639244438800L;
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

   class LoadAction extends AbstractAction {
      private static final long serialVersionUID = -6998760732995584428L;
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
                 "Load DataNode",
                 JOptionPane.YES_NO_OPTION,
                 JOptionPane.PLAIN_MESSAGE,
                 null,
                 options,
                 options[0]);
         if(userChoice == 0) {
            String userInput=fqnTextField.getText();
            load(userInput);
         }
      }
   }

   class EvictAction extends AbstractAction {
      private static final long serialVersionUID = 6007500908549034215L;
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
                 "Evict DataNode",
                 JOptionPane.YES_NO_OPTION,
                 JOptionPane.PLAIN_MESSAGE,
                 null,
                 options,
                 options[0]);
         if(userChoice == 0) {
            String userInput=fqnTextField.getText();
            evict(userInput);
         }
      }
   }


   class StartTransaction extends AbstractAction {
      private static final long serialVersionUID = 7059131008813144857L;
      public void actionPerformed(ActionEvent e) {
         if(tx_mgr == null) {
            log.error("no TransactionManager specified");
            return;
         }
         if(tx != null) {
            log.error("transaction is already running: " + tx);
            return;
         }
         try {
            tx_mgr.begin();
            tx=tx_mgr.getTransaction();
         }
         catch(Throwable t) {
            t.printStackTrace();
         }
      }
   }

   class CommitTransaction extends AbstractAction {
      private static final long serialVersionUID = 5426108920883879873L;
      public void actionPerformed(ActionEvent e) {
         if(tx == null) {
            log.error("transaction is not running");
            return;
         }
         try {
            tx.commit();
         }
         catch(Throwable t) {
            t.printStackTrace();
         }
         finally {
            tx=null;
         }
      }
   }

   class RollbackTransaction extends AbstractAction {
      private static final long serialVersionUID = -4836748411400541430L;
      public void actionPerformed(ActionEvent e) {
         if(tx == null) {
            log.error("transaction is not running");
            return;
         }
         try {
            tx.rollback();
         }
         catch(Throwable t) {
            t.printStackTrace();
         }
         finally {
            tx=null;
         }
      }
   }

   class PrintLockInfoAction extends AbstractAction {
      private static final long serialVersionUID = -2171307516592250436L;
      public void actionPerformed(ActionEvent e) {
         System.out.println("\n*** lock information ****\n" + cache_.printLockInfo());
      }
   }

   class ReleaseAllLocksAction extends AbstractAction {
      private static final long serialVersionUID = 6894888234400908985L;
      public void actionPerformed(ActionEvent e) {
         cache_.releaseAllLocks("/");
      }
   }

   class RemoveNodeAction extends AbstractAction {
      private static final long serialVersionUID = 3746013603940497991L;
      public void actionPerformed(ActionEvent e) {
         try {
            cache_.remove(selected_node);
         }
         catch(Throwable t) {
            log.error("RemoveNodeAction.actionPerformed(): " + t);
         }
      }
   }

   class AddModifyDataForNodeAction extends AbstractAction {
      private static final long serialVersionUID = -7656592171312920825L;
      public void actionPerformed(ActionEvent e) {
         Map data=getData(Fqn.fromString(selected_node));
         if(data != null && data.size() > 0) {
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
      private static final long serialVersionUID = 4882445905140460053L;
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
         tok=new StringTokenizer(fqn, TreeCacheGui2.SEP);

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
         tok=new StringTokenizer(fqn, TreeCacheGui2.SEP);

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
               sb.append(TreeCacheGui2.SEP + name);
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












