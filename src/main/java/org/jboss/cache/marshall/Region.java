/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 * Created on March 25 2003
 */
package org.jboss.cache.marshall;

import java.util.ArrayList;
import java.util.List;

/**
 * A region is a collection of tree cache nodes that share the same policy.
 * The region is specified via Fqn.
 *
 * @author Ben Wang 8-2005
 */
public class Region
{
    public static final int STATUS_ACTIVE = 0;
    public static final int STATUS_INACTIVE = 1;
    public static final int STATUS_QUEUEING = 2;

    // Region name
    private String fqn_;
    private ClassLoader cl_;
    private int status = STATUS_ACTIVE;
    private List methodCallQueue = new ArrayList();

    public Region(String fqn, ClassLoader cl)
    {
        fqn_ = fqn;
        cl_ = cl;
    }

    public Region(String fqn, ClassLoader cl, boolean inactive)
    {
        fqn_ = fqn;
        cl_ = cl;
        if (inactive)
        {
            status = STATUS_INACTIVE;
        }
    }

    public String getFqn() {return fqn_;}

    public ClassLoader getClassLoader() { return cl_; }

    public void setClassLoader(ClassLoader classLoader)
    {
        cl_ = classLoader;
    }

    public boolean isActive()
    {
        synchronized (methodCallQueue)
        {
            return (getStatus() == STATUS_ACTIVE);
        }
    }

    public boolean isInactive()
    {
        synchronized (methodCallQueue)
        {
            return (getStatus() == STATUS_INACTIVE);
        }
    }

    public boolean isQueueing()
    {
        synchronized (methodCallQueue)
        {
            return (getStatus() == STATUS_QUEUEING);
        }
    }

    public int getStatus()
    {
        return status;
    }

    public void inactivate()
    {
        synchronized (methodCallQueue)
        {
            status = STATUS_INACTIVE;
            methodCallQueue.clear();
        }
    }

    public void activate()
    {
        synchronized (methodCallQueue)
        {
            methodCallQueue.clear();
            status = STATUS_ACTIVE;
        }
    }

    public void startQueuing()
    {
        synchronized (methodCallQueue)
        {
            methodCallQueue.clear();
            status = STATUS_QUEUEING;
        }
    }

//   public void enqueue(MethodCall call)
//   {
//      methodCallQueue.add(call);
//   }

//   public void clearQueue()
//   {
//      if (queuedMethodCalls != null)
//      {
//         synchronized(queuedMethodCalls)
//         {
//            queuedMethodCalls.clear();
//         }
//      }
//   }

//   public MethodCall getQueuedMethodCall()
//   {
//      MethodCall result = null;
//      if (methodCallQueue != null && methodCallQueue.size() > 0)
//      {
//         result = (MethodCall) methodCallQueue.remove(0);
//      }
//      return result;
//   }

    public List getMethodCallQueue()
    {
        return methodCallQueue;
    }

//   public Object getStatusLock()
//   {
//      return statusLock;
//   }

}
