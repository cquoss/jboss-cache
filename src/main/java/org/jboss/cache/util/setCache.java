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
package org.jboss.cache.util;

import org.jboss.cache.TreeCache;
import org.jboss.cache.TreeCacheView2;

import bsh.CallStack;
import bsh.Interpreter;

/**
 * Custom beanshell setCache(TreeCache) command allowing beanshell user to 
 * set TreeCache reference being displayed in a JTree of TreeCacheView2 utility. 
 * Class name is intentionally violating java conventions to follow beanshell 
 * command naming conventions. 
 * 
 * @see org.jboss.cache.TreeCacheView2
 * @see http://www.beanshell.org/
 *  
 * @version $Id$
 */
public class setCache {
	public static void invoke(Interpreter env, CallStack callstack,
			TreeCache tree) {

		try {
			TreeCacheView2.setCache(tree);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
