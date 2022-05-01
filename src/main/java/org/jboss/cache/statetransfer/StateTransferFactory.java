/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.statetransfer;

import org.jboss.cache.Fqn;
import org.jboss.cache.TreeCache;
import org.jboss.cache.Version;
import org.jboss.invocation.MarshalledValueInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public abstract class StateTransferFactory
{
   private static final short RV_123 = Version.getVersionShort("1.2.3");
   private static final short RV_124 = Version.getVersionShort("1.2.4");
   private static final short RV_124SP1 = Version.getVersionShort("1.2.4.SP1");
   private static final short RV_140 = Version.getVersionShort("1.4.0");
   
   /**
    * Gets the StateTransferGenerator able to handle the given cache instance.
    * 
    * @param cache the cache
    * 
    * @return the {@link StateTransferGenerator}
    */
   public static StateTransferGenerator 
         getStateTransferGenerator(TreeCache cache)
   {
      short version = cache.getReplicationVersionShort();
      
      // Compiler won't let me use a switch
      
      // Test 1.2.4 and 1.2.4.SP1 first as these are actually lower numbers
      // than 1.2.3 since their shorts used a different algorithm
      if (version == RV_124)
         return new StateTransferGenerator_124(cache);
      else if (version == RV_124SP1)
         return new StateTransferGenerator_1241(cache);
      else if (version <= RV_123 && version > 0) // <= 0 is actually a version > 15.31.63
         return new StateTransferGenerator_123(cache);
      else if (version < RV_140 && version > 0) // <= 0 is actually a version > 15.31.63
         return new StateTransferGenerator_1241(cache);
      else
         return new StateTransferGenerator_140(cache); // current default
   }
   
   /**
    * Gets a StateTransferIntegrator able to handle the given state.
    * 
    * @param state      the state
    * @param targetFqn  Fqn of the node to which the state will be bound
    * @param cache      cache in which the state will be stored
    * @return           the {@link StateTransferIntegrator}.
    * @throws Exception
    */
   public static StateTransferIntegrator 
      getStateTransferIntegrator(byte[] state, Fqn targetFqn, TreeCache cache) 
         throws Exception
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(state);
      bais.mark(1024);      
      
      short version = 0;
      MarshalledValueInputStream in = null;
      try {
         in = new MarshalledValueInputStream(bais);
      }
      catch (IOException e) {
         // No short at the head of the stream means version 123
         version = RV_123;
      }

      try {
         if (in != null) {
            try {
               version = in.readShort();
            }
            catch (IOException io) {
               // No short at the head of the stream means version 123
               version = RV_123;
            }
         }
         
         // Compiler won't let me use a switch
         
         // Test 1.2.4 and 1.2.4.SP1 first as these are actually lower numbers
         // than 1.2.3 since their shorts used a different algorithm
         if (version == RV_124)
            return new StateTransferIntegrator_124(in, targetFqn, cache);
         else if (version == RV_124SP1)
            return new StateTransferIntegrator_1241(state, targetFqn, cache);
         else if (version <= RV_123 && version > 0) // <= 0 is actually a version > 15.31.63
            return new StateTransferIntegrator_123(state, targetFqn, cache);
         else if (version < RV_140 && version > 0) // <= 0 is actually a version > 15.31.63
            return new StateTransferIntegrator_1241(state, targetFqn, cache);
         else
            return new StateTransferIntegrator_140(state, targetFqn, cache); // current default
                 
      }
      finally {
         try {
            if (in != null) in.close();
         }
         catch (IOException io) {}
      }
   }
}
