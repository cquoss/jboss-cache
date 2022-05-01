/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.config;

import org.jboss.cache.optimistic.DataVersion;

/**
 * Used to override characteristics of specific calls to the cache.  The javadocs of each of the setters below detail functionality and behaviour.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 1.3.0
 */
public class Option
{
    private boolean failSilently;
    private boolean cacheModeLocal;
    private DataVersion dataVersion;
    private boolean suppressLocking;    
    private boolean forceDataGravitation;
    private boolean skipDataGravitation;


    /**
     *
     * @since 1.4.0
     */
    public boolean isSuppressLocking()
    {
        return suppressLocking;
    }

    /**
     * Suppresses acquiring locks for the given invocation.  Used with pessimistic locking only.  Use with extreme care, may lead to a breach in data integrity!
     * @since 1.4.0
     */
    public void setSuppressLocking(boolean suppressLocking)
    {
        this.suppressLocking = suppressLocking;
    }


    /**
     *
     * @since 1.3.0
     */
    public boolean isFailSilently()
    {
        return failSilently;
    }

    /**
     * suppress any failures in your cache operation, including version mismatches with optimistic locking, timeouts obtaining locks, transaction rollbacks.  If this is option is set, the method invocation will __never fail or throw an exception__, although it may not succeed.  With this option enabled the call will <b>not</b> participate in any ongoing transactions even if a transaction is running.
     * @since 1.3.0
     */
    public void setFailSilently(boolean failSilently)
    {
        this.failSilently = failSilently;
    }

    /**
     * only applies to put() and remove() methods on the cache.
     * @since 1.3.0
     */
    public boolean isCacheModeLocal()
    {
        return cacheModeLocal;
    }

    /**
     * overriding CacheMode from REPL_SYNC, REPL_ASYNC, INVALIDATION_SYNC, INVALIDATION_ASYNC to LOCAL.  Only applies to put() and remove() methods on the cache.
     * @since 1.3.0
     * @param cacheModeLocal
     */
    public void setCacheModeLocal(boolean cacheModeLocal)
    {
        this.cacheModeLocal = cacheModeLocal;
    }

    /**
     *
     * @since 1.3.0
     */
    public DataVersion getDataVersion()
    {
        return dataVersion;
    }

    /**
     * Passing in an {@link org.jboss.cache.optimistic.DataVersion} instance when using optimistic locking will override the default behaviour of internally generated version info and allow the caller to handle data versioning.
     * @since 1.3.0
     */
    public void setDataVersion(DataVersion dataVersion)
    {
        this.dataVersion = dataVersion;
    }

   /**
    *
    * @since 1.4.0
    */
   public boolean getForceDataGravitation()
   {
      return forceDataGravitation;
   }

   /**
    * Enables data gravitation calls if a cache miss is detected when using <a href="http://wiki.jboss.org/wiki/Wiki.jsp?page=JBossCacheBuddyReplicationDesign">Buddy Replication</a>.
    * Enabled only for a given invocation, and only useful if <code>autoDataGravitation</code> is set to <code>false</code>.
    * See <a href="http://wiki.jboss.org/wiki/Wiki.jsp?page=JBossCacheBuddyReplicationDesign">Buddy Replication</a> documentation for more details.
    * @since 1.4.0
    */
   public void setForceDataGravitation(boolean enableDataGravitation)
   {
      this.forceDataGravitation = enableDataGravitation;
   }

   /**
    * @return true if skipDataGravitation is set to true.
    * @since 1.4.1.SP6
    */
   public boolean isSkipDataGravitation()
   {
      return skipDataGravitation;
   }

   /**
    * Suppresses data gravitation when buddy replication is used.  If true, overrides {@link #setForceDataGravitation(boolean)}
    * being set to true.  Typically used to suppress gravitation calls when {@link org.jboss.cache.config.BuddyReplicationConfig#setAutoDataGravitation(boolean)}
    * is set to true.
    *
    * @param skipDataGravitation
    * @since 1.4.1.SP6
    */
   public void setSkipDataGravitation(boolean skipDataGravitation)
   {
      this.skipDataGravitation = skipDataGravitation;
   }

   public String toString()
   {
       return "Option{" +
               "failSilently=" + failSilently +
               ", cacheModeLocal=" + cacheModeLocal +
               ", dataVersion=" + dataVersion +
               ", suppressLocking=" + suppressLocking +
               ", forceDataGravitation=" + forceDataGravitation +
               ", skipDataGravitation=" + skipDataGravitation +
               '}';
   }
    
    
}
