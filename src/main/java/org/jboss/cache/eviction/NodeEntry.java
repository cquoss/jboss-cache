package org.jboss.cache.eviction;

import org.jboss.cache.Fqn;

/**
 * Value object used in queue
 *
 * @author Ben Wang 2-2004
 * @author Daniel Huang - dhuang@jboss.org
 */
public class NodeEntry
{
   private long modifiedTimeStamp;
   private long creationTimeStamp;
   private int numberOfNodeVisits;
   private int numberOfElements;
   private Fqn fqn;

   private long inUseTimeoutTimestamp;
   private boolean currentlyInUse = false;

   EvictionQueue queue;

   /**
    * Private constructor that automatically sets the creation time stamp of the node entry.
    */
   private NodeEntry()
   {
      this.creationTimeStamp = System.currentTimeMillis();
   }

   public NodeEntry(Fqn fqn)
   {
      this();
      setFqn(fqn);
   }

   public NodeEntry(String fqn)
   {
      this();
      setFqn(Fqn.fromString(fqn));
   }

   /**
    * Is the node currently in use.
    *
    * @return True/false if the node is currently marked as in use.
    */
   public boolean isCurrentlyInUse()
   {
      return currentlyInUse;
   }

   public void setCurrentlyInUse(boolean currentlyInUse, long inUseTimeout)
   {
      this.currentlyInUse = currentlyInUse;
      if (inUseTimeout > 0)
      {
         this.inUseTimeoutTimestamp = System.currentTimeMillis() + inUseTimeout;
      }
   }

   public long getInUseTimeoutTimestamp()
   {
      return this.inUseTimeoutTimestamp;
   }

   /**
    * Get modified time stamp. This stamp is created during the node is
    * processed so it has some fuzy tolerance in there.
    *
    * @return The last modified time stamp
    */
   public long getModifiedTimeStamp()
   {
      return modifiedTimeStamp;
   }

   public void setModifiedTimeStamp(long modifiedTimeStamp)
   {
      this.modifiedTimeStamp = modifiedTimeStamp;
   }

   /**
    * Get the time stamp for when the node entry was created.
    *
    * @return The node entry creation time stamp
    */
   public long getCreationTimeStamp()
   {
      return creationTimeStamp;
   }

   public void setCreationTimeStamp(long creationTimeStamp)
   {
      this.creationTimeStamp = creationTimeStamp;
   }

   public int getNumberOfNodeVisits()
   {
      return numberOfNodeVisits;
   }

   public void setNumberOfNodeVisits(int numberOfNodeVisits)
   {
      this.numberOfNodeVisits = numberOfNodeVisits;
   }

   public int getNumberOfElements()
   {
      return numberOfElements;
   }

   public void setNumberOfElements(int numberOfElements)
   {
      if (queue != null)
      {
         int difference = numberOfElements - this.numberOfElements;
         queue.modifyElementCount(difference);
      }
      this.numberOfElements = numberOfElements;
   }

   public Fqn getFqn()
   {
      return fqn;
   }

   void setFqn(Fqn fqn)
   {
      this.fqn = fqn;
   }

   public int hashCode()
   {
      return fqn.hashCode();
   }

   public boolean equals(Object o)
   {
      NodeEntry ne = (NodeEntry) o;
      return fqn.equals(ne.getFqn());
   }

   public String toString()
   {
      StringBuffer output = new StringBuffer();
      output.append("Fqn: ");
      if (fqn != null)
      {
         output.append(fqn);
      }
      else
      {
         output.append(" null");
      }

      output.append(" CreateTime: ").append(this.getCreationTimeStamp());
      output.append(" NodeVisits: ").append(this.getNumberOfNodeVisits());
      output.append(" ModifiedTime: ").append(this.getModifiedTimeStamp());
      output.append(" NumberOfElements: ").append(this.getNumberOfElements());
      output.append(" CurrentlyInUse: ").append(this.isCurrentlyInUse());
      return output.toString();
   }

}
