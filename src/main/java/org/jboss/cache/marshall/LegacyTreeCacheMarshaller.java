/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.marshall;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.blocks.MethodCall;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p/>
 * Marshaller implementation that does aplication specific marshalling in the <em>JGroups</em> <code>RpcDispatcher</code>
 * level. Application
 * that runs on specific class loader will only need to register beforehand with TreeCache the class loader
 * needed under the specific <code>fqn</code> region. Note again that this marshalling policy is region based.
 * Anything that falls under that region will use the registered class loader. We also handle the region conflict
 * during registeration time as well. For <code>fqn</code> that does
 * not belong to any region, the default (system) class loader will be used.</p>
 * <p/>
 * Heavily refactored by Manik Surtani to reflect that this is a legacy marshaller used for (occasional) region-based marshalling
 * for the JBC 1.2.x and 1.3.x series only.
 *
 * @author Ben Wang
 * @author Manik Surtani
  *
 * @version $Id: LegacyTreeCacheMarshaller.java 2192 2006-07-07 07:20:02Z bstansberry $
 */
public class LegacyTreeCacheMarshaller extends Marshaller
{

    private Log log = LogFactory.getLog(LegacyTreeCacheMarshaller.class);

    public LegacyTreeCacheMarshaller()
    {
    }

    public LegacyTreeCacheMarshaller(RegionManager manager, boolean defaultInactive, boolean useRegionBasedMarshalling)
    {
        init(manager, defaultInactive, useRegionBasedMarshalling);
    }


    public RegionManager getManager()
    {
        return regionManager;
    }

    public void setManager(RegionManager manager)
    {
        this.regionManager = manager;
    }

    public boolean isDefaultInactive()
    {
        return defaultInactive;
    }

    public void setDefaultInactive(boolean defaultInactive)
    {
        this.defaultInactive = defaultInactive;
    }

    /* ----------------- Beginning of implementation of Marshaller ---------------------- */

    /**
     * Legacy implementation of writing an object to a stream
     *
     * @param o
     * @param out
     * @throws Exception
     */
    public void objectToStream(Object o, ObjectOutputStream out) throws Exception
    {
        if (useRegionBasedMarshalling)
        {
            objectToStreamImpl(o, out);
        }
        else
        {
            //use simple serialization
            out.writeObject(externMethodCall(o));
        }
    }

    public Object objectFromStream(ObjectInputStream in) throws Exception
    {
        Object result = useRegionBasedMarshalling ? objectFromStreamImpl(in) : in.readObject();
        if (result instanceof MethodCall)
        {
            result = internMethodCall((MethodCall) result);
        }
        return result;
    }


    /**
     * Idea is to write specific fqn information in the header such that during unm-marshalling we know
     * which class loader to use.
     *
     * @param o
     * @throws Exception
     */
    private void objectToStreamImpl(Object o, ObjectOutputStream oos) throws Exception
    {
        /**
         * Object is always MethodCall, it can be either: replicate or replicateAll (used in async repl queue)
         * 1. replicate. Argument is another MethodCall. The followings are the one that we need to handle:
         * 2. replicateAll. List of MethodCalls. We can simply repeat the previous step by extract the first fqn only.
         */
        JBCMethodCall call = null;
        String fqn = null;
        
        // Just cast o to JBCMethodCall -- in the off chance its not
        // due to a subclass passing in something else, we'll just
        // deal with the CCE
        try
        {
           call = (JBCMethodCall) o; // either "replicate" or "replicateAll" now.
        
           switch (call.getMethodId())
           {
               case MethodDeclarations.replicateMethod_id:
                   fqn = extractFqnFromMethodCall(call);
                   break;
               case MethodDeclarations.replicateAllMethod_id:
                   fqn = extractFqnFromListOfMethodCall(call);
                   break;
               default :
                   throw new IllegalStateException("LegacyTreeCacheMarshaller.objectToByteBuffer(): MethodCall name is either not "
                           + " replicate or replicateAll but : " + call.getName());
           }
        }
        catch (ClassCastException cce)
        {
           if (call == null)
           {
              log.debug("Received non-JBCMethodCall " + o +" -- cannot extract Fqn so using null");
           }
           else
              throw cce;
        }
        
        // Extract fqn and write it out in fixed format
        if (fqn == null) fqn = "NULL"; // can't write null. tis can be commit.
        oos.writeUTF(fqn);
        // Serialize the rest of MethodCall object
        oos.writeObject(externMethodCall(o));
    }

    /**
     * This is the un-marshalling step. We will read in the fqn and thus obtain the user-specified classloader
     * first.
     *
     * @throws Exception
     */
    private Object objectFromStreamImpl(ObjectInputStream ois) throws Exception
    {

        // Read the fqn first
        String fqn = ois.readUTF();
        ClassLoader oldTcl = null;
        Region region = null;
        if (fqn != null && !fqn.equals("NULL"))
        {
            // obtain a region from RegionManager, if not, will use default.
            region = getRegion(fqn);

            if (region != null)
            {
                // If the region has been marked inactive, we still have
                // to return a MethodCall or RpcDispatcher will log an Error.
                // So, return a call to the TreeCache "_notifyCallOnInactive" method
                if (region.getStatus() == Region.STATUS_INACTIVE)
                {
                    if (log.isTraceEnabled())
                    {
                        log.trace("objectFromByteBuffer(): fqn: " + fqn + " is in the inactive default region");
                    }

                    return MethodCallFactory.create(MethodDeclarations.notifyCallOnInactiveMethod,
                            new Object[]{fqn});
                }

                // If the region has an associated CL, read the value using it
                ClassLoader cl = region.getClassLoader();
                if (cl != null)
                {
                    oldTcl = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(cl);

                    if (log.isTraceEnabled())
                    {
                        log.trace("objectFromByteBuffer(): fqn: " + fqn + " Will use customed class loader " + cl);
                    }
                }
            }
            else if (defaultInactive)
            {
                // No region but default inactive means region is inactive

                if (log.isTraceEnabled())
                {
                    log.trace("objectFromByteBuffer(): fqn: " + fqn + " is in an inactive region");
                }

                return MethodCallFactory.create(MethodDeclarations.notifyCallOnInactiveMethod,
                        new Object[]{fqn});
            }
        }

        // Read the MethodCall object using specified class loader
        Object obj = null;
        try
        {
            obj = ois.readObject();
        }
        finally
        {
            if (oldTcl != null)
            {
                Thread.currentThread().setContextClassLoader(oldTcl);
            }
        }

        if (obj == null)
        {
            throw new MarshallingException("Read null object with fqn: " + fqn);
        }

        // If the region is queuing messages, wrap the method call
        // and pass it to the enqueue method
        if (region != null && region.isQueueing())
        {
            obj = MethodCallFactory.create(MethodDeclarations.enqueueMethodCallMethod,
                    new Object[]{region.getFqn(), obj});
        }

        return obj;
    }

    String getColumnDump(byte buffer[])
    {
        int col = 16;
        int length = buffer.length;
        int offs = 0;
        StringBuffer sb = new StringBuffer(length * 4);
        StringBuffer tx = new StringBuffer();
        for (int i = 0; i < length; i++)
        {
            if (i % col == 0)
            {
                sb.append(tx).append('\n');
                tx.setLength(0);
            }
            byte b = buffer[i + offs];
            if (Character.isISOControl((char) b))
            {
                tx.append('.');
            }
            else
            {
                tx.append((char) b);
            }
            appendHex(sb, b);
            sb.append(' ');
        }
        int remain = col - (length % col);
        if (remain != col)
        {
            for (int i = 0; i < remain * 3; i++)
            {
                sb.append(' ');
            }
        }
        sb.append(tx);
        return sb.toString();
    }

    private static void appendHex(StringBuffer sb, byte b)
    {
        sb.append(Character.forDigit((b >> 4) & 0x0f, 16));
        sb.append(Character.forDigit(b & 0x0f, 16));
    }

    /**
     * Replace any deserialized MethodCall with our version that has
     * the correct id.
     */
    private JBCMethodCall internMethodCall(MethodCall call)
    {
        Object[] args = call.getArgs();
        JBCMethodCall result = MethodCallFactory.create(call.getMethod(), args);

        switch (result.getMethodId())
        {
            case MethodDeclarations.replicateMethod_id:
            case MethodDeclarations.clusteredGetMethod_id:
                args[0] = internMethodCall((MethodCall) args[0]);
                break;
            case MethodDeclarations.replicateAllMethod_id:
                List mods0 = (List) args[0];
                List internMods0 = new ArrayList(mods0.size());
                for (Iterator iter = mods0.iterator(); iter.hasNext();)
                {
                    internMods0.add(internMethodCall((MethodCall) iter.next()));
                }
                args[0] = internMods0;
                break;
            case MethodDeclarations.optimisticPrepareMethod_id:
            case MethodDeclarations.prepareMethod_id:
                List mods1 = (List) args[1];
                List internMods1 = new ArrayList(mods1.size());
                for (Iterator iter = mods1.iterator(); iter.hasNext();)
                {
                    internMods1.add(internMethodCall((MethodCall) iter.next()));
                }
                args[1] = internMods1;
                break;
            case MethodDeclarations.enqueueMethodCallMethod_id:
                // This actually doesn't get replicated, but to be complete...
                args[1] = internMethodCall((MethodCall) args[1]);
                break;
            default :
                break;
        }
        return result;
    }

    private Object externMethodCall(Object o)
    {
        Object toSerialize = o;
        if (o instanceof JBCMethodCall)
        {
            JBCMethodCall call = (JBCMethodCall) o;
            toSerialize = new MethodCall(call.getMethod(), (Object[]) externMethodCall(call.getArgs()));
        }
        else if (o instanceof Object[])
        {
            Object[] orig = (Object[]) o;
            toSerialize = new Object[orig.length];
            for (int i=0; i<orig.length; i++)
            {
                ((Object[])toSerialize)[i] = externMethodCall(orig[i]);
            }
        }
        else if (o instanceof List)
        {
            List orig = (List) o;
            toSerialize = new ArrayList(orig.size());
            for (int i=0; i<orig.size(); i++)
            {
                ((List) toSerialize).add(externMethodCall(orig.get(i)));
            }
        }

        return toSerialize;
    }
}
