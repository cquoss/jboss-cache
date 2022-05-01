package org.jboss.cache.lock;

import EDU.oswego.cs.dl.util.concurrent.ReentrantWriterPreferenceReadWriteLock;

/**
 * @author Bela Ban
 * @version $Id: SimpleReadWriteLock.java 4089 2007-06-29 13:33:38Z gzamarreno $
 */
public class SimpleReadWriteLock extends ReentrantWriterPreferenceReadWriteLock {

   protected synchronized Signaller endRead() {
      Signaller result=super.endRead();
//      if(result != null)
//         return result;
//
//      if(activeReaders_ == 1 && waitingWriters_ > 0) {
//         if(readers_.size() == 1 && readers_.containsKey(Thread.currentThread())) {
//            --activeReaders_;
//            return writerLock_;
//         }
//      }
//      return null;

      
      return result;
   }


}
