/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.util;

import java.io.ByteArrayOutputStream;

/**
 * Extends ByteArrayOutputStream, but exposes the internal buffer. 
 * Using this, callers don't need to call toByteArray() which copies the 
 * internal buffer.
 * <p>
 * Also overrides the superclass' behavior of always doubling the size of the
 * internal buffer any time more capacity is needed.  This class doubles the 
 * size until the internal buffer reaches a configurable max size (default is 
 * 4MB), after which it begins growing the buffer in 25% increments.  This is 
 * intended to help prevent an OutOfMemoryError during a resize of a large 
 * buffer.
 * </p>
 * <p>
 * A version of this class was originally created by Bela Ban as part of the 
 * JGroups library.
 * </p>
 * 
 * @author <a href="mailto://brian.stansberry@jboss.com">Brian Stansberry</a>
 * @version $Id$
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream
{
   /**
    * Default buffer size after which if more buffer capacity
    * is needed the buffer will grow by 25% rather than 100%
    */
   public static final int DEFAULT_DOUBLING_SIZE = 4 * 1024 * 1024; // 4MB
   
   private int maxDoublingSize = DEFAULT_DOUBLING_SIZE;
   
   public ExposedByteArrayOutputStream()
   {
      super();
   }

   public ExposedByteArrayOutputStream(int size)
   {
      super(size);
   }
   
   /**
    * Creates a new byte array output stream, with a buffer capacity of 
    * the specified size, in bytes. 
    *
    * @param   size   the initial size.
    * @param maxDoublingSize    the buffer size, after which if more capacity
    *                           is needed the buffer will grow by 25%
    *                           rather than 100%
    *                           
    * @exception  IllegalArgumentException if size is negative.
    */
   public ExposedByteArrayOutputStream(int size, int maxDoublingSize)
   {
      super(size);
      this.maxDoublingSize = maxDoublingSize;
   }

   /**
    * Gets the internal buffer array. Note that the length of this array
    * will almost certainly be longer than the data written to it; call
    * <code>size()</code> to get the number of bytes of actual data.
    */
   public byte[] getRawBuffer() {
       return buf;
   }

   public synchronized void write(byte[] b, int off, int len)
   {
      if ((off < 0) || (off > b.length) || (len < 0) ||
            ((off + len) > b.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      } 
      else if (len == 0) {
         return;
      }
      
      int newcount = count + len;
      if (newcount > buf.length) {
         byte newbuf[] = new byte[getNewBufferSize(buf.length, newcount)];
         System.arraycopy(buf, 0, newbuf, 0, count);
         buf = newbuf;
      }
      
      System.arraycopy(b, off, buf, count, len);
      count = newcount;
   }

   public synchronized void write(int b)
   {
      int newcount = count + 1;
      if (newcount > buf.length) {
         byte newbuf[] = new byte[getNewBufferSize(buf.length, newcount)];
         System.arraycopy(buf, 0, newbuf, 0, count);
         buf = newbuf;
      }
      buf[count] = (byte)b;
      count = newcount;
   }
   
   /**
    * Gets the highest internal buffer size after which if more capacity
    * is needed the buffer will grow in 25% increments rather than 100%.
    */
   public int getMaxDoublingSize()
   {
      return maxDoublingSize;
   }
   
   /**
    * Gets the number of bytes to which the internal buffer should be resized.
    * 
    * @param curSize    the current number of bytes
    * @param minNewSize the minimum number of bytes required
    * @return the size to which the internal buffer should be resized
    */
   public int getNewBufferSize(int curSize, int minNewSize)
   {
      if (curSize <= maxDoublingSize)
         return Math.max(curSize << 1, minNewSize);
      else
         return Math.max(curSize + (curSize >> 2), minNewSize);
   }

}
