/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.cache.interceptors;

/**
 * Interface capturing basic cache management statistics
 * @author Jerry Gauthier
 * @version $Id: CacheMgmtInterceptorMBean.java 1072 2006-01-26 18:26:45Z jerrygauth $
 */
public interface CacheMgmtInterceptorMBean extends InterceptorMBean
{
   /**
    * Returns the number of cache attribute hits
    * 
    * @return the number of cache hits
    */
   long getHits();
   
   /**
    * Returns the number of cache attribute misses
    * 
    * @return the number of cache misses
    */
   long getMisses();
   
   /**
    * Returns the number of cache attribute put operations
    * 
    * @return the number of cache put operations
    */
   long getStores();
   
   /**
    * Returns the number of cache eviction operations
    * 
    * @return the number of cache eviction operations
    */
   long getEvictions();
   
   /**
    * @see org.jboss.cache.TreeCacheMBean#getNumberOfAttributes()
    */
   int getNumberOfAttributes();
   
   /**
    * @see org.jboss.cache.TreeCacheMBean#getNumberOfNodes()
    */
   int getNumberOfNodes();
   
   /**
    * Returns the hit/miss ratio for the cache
    * This ratio is defined as hits/(hits + misses)
    * 
    * @return the hit/miss ratio for the cache
    */
   double getHitMissRatio();
   
   /**
    * Returns the read/write ratio for the cache
    * This ratio is defined as (hits + misses)/stores
    * 
    * @return the read/writes ratio for the cache
    */
   double getReadWriteRatio();
   
   /**
    * Returns average milliseconds for an attribute read operation
    * This includes both hits and misses.
    * 
    * @return the average number of milliseconds for a read operation
    */
   long getAverageReadTime();
   
   /**
    * Returns average milliseconds for an attribute write operation
    * 
    * @return the average number of milliseconds for a write operation
    */
   long getAverageWriteTime();
   
   /**
    * Returns seconds since cache started
    * 
    * @return the number of seconds since the cache was started
    */
   long getElapsedTime();
   
   /**
    * Returns seconds since cache statistics reset
    * If statistics haven't been reset, this will be the same as ElapsedTime
    * 
    * @return the number of seconds since the cache statistics were last reset
    */
   long getTimeSinceReset();
}
