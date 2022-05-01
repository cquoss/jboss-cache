package org.jboss.cache.lock;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Map which reduces concurrency and potential memory leaks for non-static ThreadLocals.
 * http://www.me.umn.edu/~shivane/blogs/cafefeed/2004/06/of-non-static-threadlocals-and-memory.html
 * @author Brian Dueck
 * @version $Id: ThreadLocalMap.java 166 2005-06-17 05:49:03Z bela $
 */
public class ThreadLocalMap implements Map {

    private ThreadLocal threadLocal = new ThreadLocal();

    private Map getThreadLocalMap() {
        Map map = (Map) threadLocal.get();
        if (map == null) {
            map = new HashMap();
            threadLocal.set(map);
        }
        return map;
    }

    public Object put(Object key, Object value) {
        return getThreadLocalMap().put(key, value);
    }

    public Object get(Object key) {
        return getThreadLocalMap().get(key);
    }

    public Object remove(Object key) {
        return getThreadLocalMap().remove(key);
    }

    public int size() {
        return getThreadLocalMap().size();
    }

    public void clear() {
        getThreadLocalMap().clear();        
    }

    public boolean isEmpty() {
        return getThreadLocalMap().isEmpty();
    }

    public boolean containsKey(Object arg0) {
        return getThreadLocalMap().containsKey(arg0);
    }

    public boolean containsValue(Object arg0) {
        return getThreadLocalMap().containsValue(arg0);
    }

    public Collection values() {
        return getThreadLocalMap().values();
    }

    public void putAll(Map arg0) {
        getThreadLocalMap().putAll(arg0);
    }

    public Set entrySet() {
        return getThreadLocalMap().entrySet();        
    }

    public Set keySet() {
        return getThreadLocalMap().keySet();
    }

}