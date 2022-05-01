/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop.util;

import java.io.Serializable;

/**
 * Represent null values (based on org.jboss.util.Null)
 *
 * @author Scott Marlow
 */
public final class Null implements Serializable
{
   static final long serialVersionUID = -402153636437493134L;
   /** singleton instance of Null. */
   private static final org.jboss.cache.aop.util.Null NULL_OBJECT_MARKER = new org.jboss.cache.aop.util.Null();

   // String representation of Null
   private static final String NULL_AS_STRING_REPRESENTATION = "jboss.cache._NULL_";

   /** Do not allow public construction. */
   private Null() {}


   /**
    * Represents null as a special Null object marker.
    * @param aObject
    * @return if aObject is null, return Null.NULL_OBJECT_MARKER otherwise return aObject.
    */
   public final static Object toNullObject( Object aObject )
   {
      if ( aObject == null ) {
         return org.jboss.cache.aop.util.Null.NULL_OBJECT_MARKER;
      }
      else {
         return aObject;
      }
   }

   /**
    *  Represents null key as a special string value.
    * @param aObject
    * @return if aObject is null, return Null.NULL_AS_STRING_REPRESENTATION otherwise return aObject.
    */
   public final static Object toNullKeyObject( Object aObject )
   {
      if ( aObject == null ) {
         return org.jboss.cache.aop.util.Null.NULL_AS_STRING_REPRESENTATION;
      }
      else {
         return aObject;
      }
   }

   /**
    * If the passed object represents null (instance of Null.NULL_OBJECT_MARKER), will replace with null value.
    * @param aObject
    * @return null if aObject is instance of Null.NULL_OBJECT_MARKER, otherwise return aObject.
    */
   public final static Object toNullValue( Object aObject )
   {
      if ( org.jboss.cache.aop.util.Null.NULL_OBJECT_MARKER.equals( aObject ) ) {
         return null;
      }
      else {
         return aObject;
      }
   }

   /**
    * Converts Null string representation back to null value.
    * @param aObject
    * @return null if aObject represents a null, otherwise return aObject.
    */
   public final static Object toNullKeyValue( Object aObject )
   {
      if ( org.jboss.cache.aop.util.Null.NULL_AS_STRING_REPRESENTATION.equals( aObject ) ) {
         return null;
      }
      else {
         return aObject;
      }
   }


   /**
    * Return a string representation of null value.
    *
    * @return Null.NULL_AS_STRING_REPRESENTATION;
    */
   public String toString() {
      return NULL_AS_STRING_REPRESENTATION;
   }

   /**
    * Hashcode of unknown (null) value will be zero.
    *
    * @return  Zero.
    */
   public int hashCode() {
      return 0;
   }

   /**
    * Check if the given object is a Null instance or <tt>null</tt>.
    *
    * @param obj  Object to test.
    * @return     True if the given object is a Null instance or <tt>null</tt>.
    */
   public boolean equals(final Object obj) {
      if (obj == this) return true;
      return (obj == null || obj.getClass() == getClass() || NULL_AS_STRING_REPRESENTATION.equals(obj));
   }
}
