/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.optimistic.DataVersion;
import org.jboss.cache.optimistic.DefaultDataVersion;

import java.util.Map;

/**
 * OptimisticTreeNode contains a data version. 
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class OptimisticTreeNode extends Node
{

    private DataVersion version = DefaultDataVersion.ZERO;

    private static Log log = LogFactory.getLog(OptimisticTreeNode.class);

    private static final long serialVersionUID = 7000622696587912654L;

    /**
     * Although this object has a reference to the TreeCache, the optimistic
     * node is actually disconnected from the TreeCache itself.
     * The parent could be looked up from the TransactionWorkspace.
     */
    private Node parent;

    public OptimisticTreeNode()
    {
    }

    /**
     * @param child_name
     * @param fqn
     * @param parent
     * @param data
     * @param cache
     */
    public OptimisticTreeNode(Object child_name, Fqn fqn, Node parent, Map data, TreeCache cache)
    {
        super(child_name, fqn, parent, data, cache);
        this.parent = parent;
    }

    /**
     * @param child_name
     * @param fqn
     * @param parent
     * @param data
     * @param cache
     */
    public OptimisticTreeNode(Object child_name, Fqn fqn, Node parent, Map data, boolean mapSafe, TreeCache cache)
    {
        super(child_name, fqn, parent, data, mapSafe, cache);
        this.parent = parent;
    }

    /**
     * @param child_name
     * @param fqn
     * @param parent
     * @param key
     * @param value
     * @param cache
     */
    public OptimisticTreeNode(Object child_name, Fqn fqn, Node parent, Object key, Object value, TreeCache cache)
    {
        super(child_name, fqn, parent, key, value, cache);
        this.parent = parent;
    }

    public OptimisticTreeNode(Object childName, Fqn fqn, Node parent, Map data, boolean mapSafe, TreeCache cache, DataVersion version)
    {
        super(childName, fqn, parent, data, mapSafe, cache);
        this.parent = parent;
    }

    /**
     * Returns the version id of this node.
     * @return the version
     */
    public DataVersion getVersion()
    {
       return version;
    }

    /**
     * Returns the parent.
     */
    public TreeNode getParent()
    {
       return parent;
    }

    /**
     * Sets the version id of this node.
     * @param version
     */
    public void setVersion(DataVersion version)
    {
       this.version = version;
    }

    public String toString()
    {
        return super.toString() + "\n(Optimistically locked node)\n";
    }

   public boolean equals(Object other)
   {
      if (other != null && other instanceof OptimisticTreeNode)
      {
         if (other == this) return true;
         Fqn otherFqn = ((OptimisticTreeNode) other).getFqn();
         if (fqn == null) return otherFqn == null;
         return fqn.equals(otherFqn);
      }
      return false;
   }

   public int hashCode()
   {
      if (fqn == null) return 100;
      return fqn.hashCode() * 22;
   }   
}