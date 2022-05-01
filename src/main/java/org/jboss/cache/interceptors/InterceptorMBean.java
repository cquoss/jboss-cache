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

import java.util.Map;

/**
 * Interface containing common cache management operations
 * @author Jerry Gauthier
 * @version $Id: InterceptorMBean.java 940 2005-12-23 21:01:40Z jerrygauth $
 */
public interface InterceptorMBean
{
   /**
    * Returns whether an interceptor's statistics are
    * being captured.
    * 
    * @return true if statistics are captured
    */
   boolean getStatisticsEnabled();
   
   /**
    * Enables an interceptor's cache statistics
    * If true, the interceptor will capture statistics
    * and make them available through the mbean.
    * 
    *  @param enabled true if statistics should be captured
    */
   void setStatisticsEnabled(boolean enabled);

   /**
    * Returns a map of the cache interceptor's statistics
    * Map elements consist of {String name, Object value} entries
    * 
    * @return a map containing statistics
    */
   Map dumpStatistics();
   
   /**
    * Resets an interceptor's cache statistics
    */
   void resetStatistics();
}
