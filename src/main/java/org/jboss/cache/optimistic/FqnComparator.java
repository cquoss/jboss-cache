/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.optimistic;

import org.jboss.cache.Fqn;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares the order of two FQN.
 * Sorts by name, then by depth, e.g. 
 <pre>
 aaa/bbb
 xxx
 xxx/ccc 
 </pre>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Steve Woodcock (<a href="mailto:stevew@jofti.com">stevew@jofti.com</a>)
 */
public class FqnComparator implements Comparator, Serializable {

   static final long serialVersionUID = -102183636437493134L;

   public static final FqnComparator INSTANCE = new FqnComparator();

   /**
    * Sorts by name, then depth.
    */
   public FqnComparator() {
   }

	/**
   * Returns -1 if the first comes before; 0 if they are the same; 1 if the
   * second Fqn comes before.  <code>null</code> always comes first.
	 */
	public int compare(Object o1, Object o2) {
		Fqn fqn1 = (Fqn)o1;
		Fqn fqn2 = (Fqn)o2;
    int s1 = fqn1.size();
    int s2 = fqn2.size();

    if (s1 == 0) {
      return (s2 == 0) ? 0 : -1;
    }

		if (s2 == 0) {
			return 1;
		}

    int size = Math.min(s1, s2);

		for (int i = 0; i < size; i++) {
      Object e1 = fqn1.get(i);
      Object e2 = fqn2.get(i);
      if (e1 == e2)
         continue;
      if (e1 == null)
        return 0;
      if (e2 == null)
        return 1;
			if (!e1.equals(e2)) {
        int c = compareElements(e1, e2);
        if (c != 0)
          return c;
			}
		}

    return s1 - s2;
	}

  /**
   * Compares two Fqn elements.
   * If e1 and e2 are the same class and e1 implements Comparable,
   * returns e1.compareTo(e2).
   * Otherwise, returns e1.toString().compareTo(e2.toString()).
   */
  protected int compareElements(Object e1, Object e2) {
     if (e1.getClass() == e2.getClass() && e1 instanceof Comparable) {
        return ((Comparable)e1).compareTo(e2);
     } else {
        return e1.toString().compareTo(e2.toString());
     }
  }


}
