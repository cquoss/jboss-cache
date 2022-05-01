package org.jboss.cache.aop.eviction;

import org.jboss.cache.Fqn;
import org.jboss.cache.eviction.LRUAlgorithm;
import org.jboss.cache.eviction.EvictionPolicy;
import org.jboss.cache.eviction.EvictionException;
import org.jboss.cache.eviction.NodeEntry;
import org.jboss.cache.aop.AOPInstance;

import java.util.Iterator;
import java.util.Set;

/**
 * LRUAlgorithm specific to PojoCache. Overriding couple of hooks to customize
 * the algorithm such that it works correctly when using PojoCache.
 * The basic strategy for the AOP-specific case are:
 * <ul>
 * <li>When a node is visited, it will check if it is an AOPInstance node. If it
 * is, then it is an AOP node. In that case, we will update all children nodes'
 * time stamp to synchronize with parent node.</li>
 * <li>When a node is to be evicted, it will check if it an AOP node. If it is,
 * we will traverse through the children nodes to see if their timestamp is younger.
 * If it is younger, then we must not evict the whol aop node (i.e., parent node is
 * not evicted either). Furthermore, we should synchronize the whole tree.
 * </ul>
 *
 * @author Ben Wang, Feb 17, 2004
 */
public class AopLRUAlgorithm extends LRUAlgorithm
{
}
