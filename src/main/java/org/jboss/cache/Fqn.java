/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache;


import org.apache.commons.logging.LogFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Fully qualified name.  A list of relatives names, which can be any Object,
 * from root to a given node.  This class is immutable.
 * <p />
 * Note that<br>
 * <p />
 * <code>Fqn f = new Fqn("/a/b/c");</code>
 * <p />
 * is <b>not</b> the same as
 * <p />
 * <code>Fqn f = Fqn.fromString("/a/b/c");</code>
 * <p />
 * The former will result in a single Fqn, called "/a/b/c" which hangs directly under Fqn.ROOT.
 * <p />
 * The latter will result in 3 Fqns, called "a", "b" and "c", where "c" is a child of "b", "b" is a child of "a", and "a" hangs off Fqn.ROOT.
 * <p />
 * Another way to look at it is that the "/" separarator is only parsed when it form sa  part of a String passed in to Fqn.fromString() and not otherwise.
 * @version $Revision: 2341 $
 */
public class Fqn implements Cloneable, Externalizable {

   /**
    * Separator between FQN elements.
    */
   public static final String SEPARATOR = "/";

   private List elements;
   private transient int hash_code=0;

   /** 
    * Controls the implementation of read/writeExternal.
    * Package protected so TreeCache can set it when ReplicationVersion is set.
    */
   static boolean REL_123_COMPATIBLE = false;
   
   static
   {
      // Check if they set a system property telling us to use 1.2.3 compatibility mode
      // Obscure use case for this: Server has multiple caches, only one of which has
      // TreeCache.setReplicationVersion() set to "1.2.3".  If the 1.2.4+ caches start
      // first, they will begin exchanging 1.2.4 style fqn's, and then when the 1.2.3
      // cache starts the format will change when it changes REL_123_COMPATIBLE.  This
      // could cause chaos. The system property allows the 1.2.3 mode to be used by 
      // the 1.2.4 caches from the start.
      try
      {
         AccessController.doPrivileged(
            new PrivilegedAction()
            {
               public Object run()
               {
                  String compatible = System.getProperty("jboss.cache.fqn.123compatible");
                  REL_123_COMPATIBLE = Boolean.valueOf(compatible).booleanValue();
                  return null;
               }
            });
      }
      catch (SecurityException ignored) 
      {
         // they just can't use system properties
      }
      catch (Throwable t)
      {
         LogFactory.getLog(Fqn.class).error("Caught throwable reading system property " +
                                            "jboss.cache.fqn.123compatible", t);
      }
   }
   
   private static final long serialVersionUID = -5351930616956603651L;

   /**
    * Immutable root FQN.
    */
   public static final Fqn ROOT = new Fqn();

   /**
    * Constructs a root FQN.
    */
   public Fqn() {
      elements = Collections.EMPTY_LIST;
   }

   /**
    * Constructs a single element Fqn.  Note that if a String is passed in, separator characters {@link #SEPARATOR} are <b>not</b> parsed.
    * If you wish these to be parsed, use {@link #fromString(String)}.
    * @see #fromString(String)
    */
   public Fqn(Object name) {
      elements = Collections.singletonList(name);
   }

   /**
    * Constructs a FQN from a list of names.
    */
   public Fqn(List names) {
      if (names != null)
         elements = new ArrayList(names);
      else
         elements = Collections.EMPTY_LIST;
   }

   /**
    * Constructs a FQN from an array of names.
    */
   public Fqn(Object[] names) {
      if (names == null)
         elements = Collections.EMPTY_LIST;
      else {
         elements = Arrays.asList(names);
      }
   }

   /**
    * Constructs a FQN from a base and relative name.
    */
   public Fqn(Fqn base, Object relative_name) {
      elements = new ArrayList(base.elements.size() + 1);
      elements.addAll(base.elements);
      elements.add(relative_name);
   }

   /**
    * Constructs a FQN from a base and relative FQN.
    */
   public Fqn(Fqn base, Fqn relative) {
      this(base, relative.elements);
   }

   /**
    * Constructs a FQN from a base and a list of relative names.
    */
   public Fqn(Fqn base, List relative) {
      elements = new ArrayList(base.elements.size() + relative.size());
      elements.addAll(base.elements);
      elements.addAll(relative);
   }

   /**
    * Constructs a FQN from a base and two relative names.
    */
   public Fqn(Fqn base, Object relative_name1, Object relative_name2) {
      elements = new ArrayList(base.elements.size() + 2);
      elements.addAll(base.elements);
      elements.add(relative_name1);
      elements.add(relative_name2);
   }

   /**
    * Constructs a FQN from a base and three relative names.
    */
   public Fqn(Fqn base, Object relative_name1, Object relative_name2, Object relative_name3) {
      elements = new ArrayList(base.elements.size() + 3);
      elements.addAll(base.elements);
      elements.add(relative_name1);
      elements.add(relative_name2);
      elements.add(relative_name3);
   }

   /**
    * Construct from an unmodifiable list.
    * For internal use.
    */
   private static Fqn createFqn(List list) {
      Fqn fqn = new Fqn();
      fqn.elements = list;
      return fqn;
   }

   /**
    * Returns a new FQN from a string, where the elements are deliminated by
    * one or more separator ({@link #SEPARATOR}) characters.<br><br>
    * Example use:<br>
    * <pre>
    * Fqn.fromString("/a//b/c/");
    * </pre><br>
    * is equivalent to:<br>
    * <pre>
    * new Fqn(new Object[] { "a", "b", "c" });
    * </pre><br>
    * but not<br>
    * <pre>
    * new Fqn("/a/b/c");
    * </pre>
    *
    * @see #Fqn(Object[])
    * @see #Fqn(Object)
    */
   public static Fqn fromString(String fqn) {
      if (fqn == null) 
         return ROOT;
      return createFqn(parse(fqn));
   }

   private static List parse(String fqn) {
      List list = new ArrayList();
      StringTokenizer tok = new StringTokenizer(fqn, SEPARATOR);
      while (tok.hasMoreTokens()) {
         list.add(tok.nextToken());
      }
      return list;
   }

   /**
    * Obtains a child Fqn from a sub-index.
    *
    * @param index where is the last index of child fqn
    * @return A Fqn child object.
    */
   public Fqn getFqnChild(int index) {
      return createFqn(elements.subList(0, index));
   }

   /**
    * Obtains a child Fqn from a sub-index.
    */
   public Fqn getFqnChild(int startIndex, int endIndex)
   {
      return createFqn(elements.subList(startIndex, endIndex));
   }

   /**
    * Returns the number of elements in the FQN.
    * The root node contains zero.
    */
   public int size() {
      return elements.size();
   }

   /**
    * Returns the Nth element in the FQN.
    */
   public Object get(int index) {
      return elements.get(index);
   }

   /**
    * Returns the last element in the FQN.
    * @see #getName
    */
   public Object getLast() {
      if (isRoot()) return SEPARATOR; 
      return elements.get(elements.size() - 1);
   }

   /**
    * Returns true if the FQN contains this element.
    */
   public boolean hasElement(Object o) {
      return elements.lastIndexOf(o) != -1;
   }

   /**
    * Clones the FQN.
    */
   public Object clone() {
      return new Fqn(elements);
   }

   /**
    * Returns true if obj is a FQN with the same elements.
    */
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if(!(obj instanceof Fqn))
         return false;
      Fqn other=(Fqn)obj;
      return elements.equals(other.elements);
   }

   /**
    * Returns a hash code with FQN elements.
    */
   public int hashCode() {
      if (hash_code == 0) {
         hash_code=_hashCode();
      }
      return hash_code;
   }

   /**
    * Returns this FQN as a string, prefixing the first element with a / and
    * joining each subsequent element with a /.
    * If this is the root FQN, returns {@link Fqn#SEPARATOR}.
    * Example:
    <pre>
    new Fqn(new Object[] { "a", "b", "c" }).toString(); // "/a/b/c"
    Fqn.ROOT.toString(); // "/"
    </pre>
    */
   public String toString()
   {
      if (isRoot())
         return TreeCache.SEPARATOR;
      StringBuffer sb = new StringBuffer();
      for (Iterator it = elements.iterator(); it.hasNext();)
      {
         sb.append(TreeCache.SEPARATOR).append(it.next());
      }
      return sb.toString();
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      
	  if (REL_123_COMPATIBLE)
	  {
		 out.writeObject(elements);
	  }
	  else
	  {
         out.writeShort(elements.size());
         Object element;
         for(Iterator it=elements.iterator(); it.hasNext();) {
            element=it.next();
            out.writeObject(element);
         }
	  }
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      
      if (REL_123_COMPATIBLE)
      {
         elements=(List)in.readObject();
      }
      else
      {
         short length = in.readShort();
         this.elements = new ArrayList(length);
         for (int i=0; i < length; i++) {
            elements.add(in.readObject());
         }
      }
   }


   /**
    * Returns true if this fqn is child of parentFqn
    *
    * @param parentFqn
    * @return true if the target is a child of parentFqn
    */
   public boolean isChildOf(Fqn parentFqn) {
      if (parentFqn.elements.size() == elements.size())
         return false;
      return isChildOrEquals(parentFqn);
   }

   /**
    * Returns true if this fqn is equals or the child of parentFqn.
    */
   public boolean isChildOrEquals(Fqn parentFqn) {
      List parentList = parentFqn.elements;
      if (parentList.size() > elements.size())
         return false;
      for (int i = parentList.size() - 1; i >= 0; i--) {
         if (!parentList.get(i).equals(elements.get(i)))
            return false;
      }
      return true;
   }

   /**
    * Calculates a hash code by summing the hash code of all elements.
    */ 
   private int _hashCode() {
      int hashCode = 0;
      int count = 1; 
      Object o;
      for (Iterator it=elements.iterator(); it.hasNext();) {
         o = it.next();
         hashCode += (o == null) ? 0 : o.hashCode() * count++;
      }
      if (hashCode == 0) // fix degenerate case
         hashCode = 0xFEED;
      return hashCode;
   }

   /**
    * Returns the parent of this FQN.
    */
   public Fqn getParent()
   {
       switch (elements.size())
       {
           case 0:
           case 1:
               return ROOT;
           default:
               return createFqn(elements.subList(0, elements.size() - 1));
       }
   }

   /**
    * Returns true if this is a root FQN.
    */
   public boolean isRoot()
   {
       return elements.isEmpty();
   }

   /**
    * Returns a String representation of the last element that makes up this Fqn.
    * If this is the root, returns {@link Fqn#SEPARATOR}.
    */
   public String getName()
   {
       if (isRoot()) return SEPARATOR;
       else return String.valueOf(getLast());
   }

   /**
    * Peeks into the elements that build up this Fqn.  The list returned is
    * read-only, to maintain the immutable nature of Fqn.
    * @return an unmodifiable list
    */
   public List peekElements()
   {
       return Collections.unmodifiableList( elements );
   }
}
