package org.jboss.cache.loader.jdbm;

/**
 * Special FQN entry to indicate null.
 */
class Null implements java.io.Serializable {

   private static final long serialVersionUID = -1337133713374690630L;

   static final Object NULL = new Null();

   private Null() {}

   public String toString() {
      return "null";
   }

   Object readResolve() {
      return NULL;
   }

}
