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
 * Interface capturing transaction statistics
 * @author Jerry Gauthier
 * @version $Id: TxInterceptorMBean.java 1071 2006-01-25 19:26:40Z jerrygauth $
 */
public interface TxInterceptorMBean extends InterceptorMBean
{
   /**
    * Returns the number of transaction prepares
    * 
    * @return the number of prepares
    */
   long getPrepares();
   
   /**
    * Returns the number of transaction commits
    * 
    * @return the number of commits
    */
   long getCommits();
   
   /**
    * Returns the number of transaction rollbacks
    * 
    * @return the number of rollbacks
    */
   long getRollbacks();

}
