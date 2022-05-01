/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.aop;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import org.jboss.cache.Fqn;

/**
 * Wrapper type for cached AOP instances.
 * When an object is looked up or put in TreeCacheAOP, this object will be advised with a CacheInterceptor.
 * The tree cache stores a reference to this object (for example to update the instance variables, etc.).
 * Since this reference need to be transactional but never replicated (the reference is only valid within the VM)
 * this reference is wrapped into an AOPInstance.
 * In addition, this instance also serves as a metadata for PojoCache. E.g., it has a reference count for
 * multiple references and reference FQN.
 *
 * @author Harald Gliebe
 * @author Ben Wang
 */
public class AOPInstance implements Serializable // Use Serializable since Externalizable is not smaller
{
//    protected static Log log=LogFactory.getLog(AOPInstance.class.getName());
   public static final Object KEY = "AOPInstance";
   public static final int INITIAL_COUNTER_VALUE = -1;

   static final long serialVersionUID = 6492134565825613209L;

   // The instance is transient to avoid reflection outside the VM
   protected transient Object instance_;

   // If not null, it signifies that this is a reference that points to this fqn.
   // Note that this will get replicated.
   protected String refFqn_ = null;

   // Reference counting. THis will get replicated as well. This keep track of number of other instances
   // that referenced this fqn.
   protected int refCount_ = INITIAL_COUNTER_VALUE;

   // List of fqns that reference this fqn. Assume list size is not big.
   protected List referencingFqnList_ = null;

   public AOPInstance()
   {
   }

   public AOPInstance(Object instance)
   {
      set(instance);
   }

   public AOPInstance copy()
   {
       AOPInstance objCopy = new AOPInstance(instance_);
       objCopy.refCount_ = refCount_;
       objCopy.refFqn_ = refFqn_;
       objCopy.referencingFqnList_ = (referencingFqnList_ == null ? null : new ArrayList(referencingFqnList_));
       return objCopy;
   }
   
   Object get()
   {
      return instance_;
   }

   void set(Object instance)
   {
      instance_ = instance;
   }

   String getRefFqn()
   {
      return refFqn_;
   }

   void setRefFqn(String refFqn)
   {
      refFqn_ = refFqn;
   }

   void removeRefFqn()
   {
      refFqn_ = null;
   }

   synchronized int incrementRefCount(Fqn referencingFqn)
   {
      if(referencingFqn != null)
      {
         if( referencingFqnList_ == null)
         {
            referencingFqnList_ = new ArrayList();
         }

         if(referencingFqnList_.contains(referencingFqn))
            throw new IllegalStateException("AOPInstance.incrementRefCount(): source fqn: " +
                    referencingFqn + " is already present.");

         referencingFqnList_.add(referencingFqn);
      }

      refCount_ += 1;
//logger_.info("incrementRefCount(): current ref count " +refCount_);
      return refCount_;
   }

   synchronized int decrementRefCount(Fqn sourceFqn)
   {

      if(sourceFqn != null)
      {
         if(!referencingFqnList_.contains(sourceFqn))
            throw new IllegalStateException("AOPInstance.decrementRefCount(): source fqn: " +
                    sourceFqn + " is not present.");

         referencingFqnList_.remove(sourceFqn);
      }

      refCount_ -= 1;
//logger_.info("decrementRefCount(): current ref count " +refCount_);
      return refCount_;
   }

   synchronized int getRefCount()
   {
      return refCount_;
   }

   synchronized Fqn getAndRemoveFirstFqnInList()
   {
      return (Fqn)referencingFqnList_.remove(0);
   }

   synchronized void addFqnIntoList(Fqn fqn)
   {
      referencingFqnList_.add(0, fqn);
   }
}
