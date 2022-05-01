/**
 *
 * @author Bela Ban Nov 25, 2003
 * @version $Id: IsolationLevel.java 2015 2006-05-31 14:08:39Z msurtani $
 */
package org.jboss.cache.lock;

/**
 * Various transaction isolation levels as an enumerated class.
 */
public class IsolationLevel
{
   public static final IsolationLevel NONE = new IsolationLevel("NONE");
   public static final IsolationLevel SERIALIZABLE = new IsolationLevel("SERIALIZABLE");
   public static final IsolationLevel REPEATABLE_READ = new IsolationLevel("REPEATABLE_READ");
   public static final IsolationLevel READ_COMMITTED = new IsolationLevel("READ_COMMITTED");
   public static final IsolationLevel READ_UNCOMMITTED = new IsolationLevel("READ_UNCOMMITTED");

   private final String myName; // for debug only

   private IsolationLevel(String name)
   {
      myName = name;
   }

   /**
    * Returns the level as a string.
    */
   public String toString()
   {
      return myName;
   }

   /**
    * Returns an isolation level from a string.
    * Returns null if not found.
    */
   public static IsolationLevel stringToIsolationLevel(String level)
   {
      if (level == null) return null;
      level = level.toLowerCase().trim();
      if (level.equals("none")) return NONE;
      if (level.equals("serializable")) return SERIALIZABLE;
      if (level.equals("repeatable_read") || level.equals("repeatable-read")) return REPEATABLE_READ;
      if (level.equals("read_committed") || level.equals("read-committed")) return READ_COMMITTED;
      if (level.equals("read_uncommitted") || level.equals("read-uncommitted")) return READ_UNCOMMITTED;
      return null;
   }
}
