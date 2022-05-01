/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.marshall;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.cache.CacheException;
import org.jboss.cache.Fqn;
import org.jboss.cache.GlobalTransaction;
import org.jgroups.Address;
import org.jgroups.blocks.MethodCall;
import org.jgroups.stack.IpAddress;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * An enhanced marshaller for RPC calls between TreeCache instances.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class TreeCacheMarshaller140 extends Marshaller
{
    // logger
    private static Log log = LogFactory.getLog(TreeCacheMarshaller140.class);

    // magic numbers
    protected static final int MAGICNUMBER_METHODCALL = 1;
    protected static final int MAGICNUMBER_FQN = 2;
    protected static final int MAGICNUMBER_GTX = 3;
    protected static final int MAGICNUMBER_IPADDRESS = 4;
    protected static final int MAGICNUMBER_ARRAY_LIST = 5;
    protected static final int MAGICNUMBER_INTEGER = 6;
    protected static final int MAGICNUMBER_LONG = 7;
    protected static final int MAGICNUMBER_BOOLEAN = 8;
    protected static final int MAGICNUMBER_STRING = 9;
    protected static final int MAGICNUMBER_LINKED_LIST = 10;
    protected static final int MAGICNUMBER_HASH_MAP = 11;
    protected static final int MAGICNUMBER_TREE_MAP = 12;
    protected static final int MAGICNUMBER_HASH_SET = 13;
    protected static final int MAGICNUMBER_TREE_SET = 14;
    protected static final int MAGICNUMBER_NULL = 99;
    protected static final int MAGICNUMBER_SERIALIZABLE = 100;
    protected static final int MAGICNUMBER_REF = 101;

    public TreeCacheMarshaller140(RegionManager manager, boolean defaultInactive, boolean useRegionBasedMarshalling)
    {
        init(manager, defaultInactive, useRegionBasedMarshalling);
        if (useRegionBasedMarshalling)
        {
            log.debug("Using region based marshalling logic : marshalling Fqn as a String first for every call.");
        }
    }

    // -------- Marshaller interface

    public void objectToStream(Object o, ObjectOutputStream out) throws Exception
    {
        if (log.isTraceEnabled()) log.trace("Marshalling object " + o);
        Map refMap = new HashMap();

        if (useRegionBasedMarshalling)
        {
            // we first marshall the Fqn as a String (ugh!)
            
            // Just cast o to JBCMethodCall -- in the off chance its not
            // due to a subclass passing in something else, we'll just
            // deal with the CCE
           JBCMethodCall call = null;
           try
           {
              call = (JBCMethodCall) o;
              String fqnAsString = extractFqnAsString(call);
              marshallObject(fqnAsString, out, refMap);
           }
           catch (ClassCastException cce)
           {
              if (call == null)
              {
                 log.debug("Received non-JBCMethodCall " + o +" -- cannot extract Fqn so using null");
                 marshallObject(null, out, refMap);
              }
              else
                 throw cce;
           }
        }

        marshallObject(o, out, refMap);
    }

    public Object objectFromStream(ObjectInputStream in) throws Exception
    {
        Object retValue;
        Map refMap = new HashMap();

        if (useRegionBasedMarshalling)
        {
            // first unmarshall the fqn as a String
            // This may be null if the call being unmarshalled is
            // not region-based
            String fqn = (String) unmarshallObject(in, refMap);
            try
            {
                Region region = null;
                if (fqn != null)
                {
                    region = findRegion(fqn);
                }
                retValue = region == null ? unmarshallObject(in, refMap) : unmarshallObject(in, region.getClassLoader(), refMap);
                if (region != null && region.isQueueing())
                {
                    Object originalRetValue = retValue;
                    if (log.isDebugEnabled())
                    {
                        log.debug("Received call on an ququing Fqn region (" + fqn + ").  Calling enqueueMethodCallMethod");
                    }
                    retValue = MethodCallFactory.create(MethodDeclarations.enqueueMethodCallMethod, new Object[]{fqn, originalRetValue});
                }
            }
            catch (InactiveRegionException e)
            {
                if (log.isDebugEnabled())
                {
                    log.debug("Received call on an inactive Fqn region (" + fqn + ").  Calling notifyCallOnInactiveMetod");
                }
                retValue = MethodCallFactory.create(MethodDeclarations.notifyCallOnInactiveMethod, new Object[]{fqn});
            }
        }
        else
        {
            retValue = unmarshallObject(in, refMap);
        }

        return retValue;
    }

    private Region findRegion(String fqn) throws InactiveRegionException
    {
        Region region;
        // obtain a region from RegionManager, if not, will use default.
        region = fqn == null ? null : getRegion(fqn);

        if (region != null)
        {
            // If the region has been marked inactive, we still have
            // to return a MethodCall or RpcDispatcher will log an Error.
            // So, return a call to the TreeCache "_notifyCallOnInactive" method
            if (region.getStatus() == Region.STATUS_INACTIVE)
            {
                throw new InactiveRegionException();
            }

        }
        else if (defaultInactive)
        {
            // No region but default inactive means region is inactive
            throw new InactiveRegionException();
        }

        return region;
    }

    private String extractFqnAsString(JBCMethodCall call) throws Exception
    {
        String fqnAsString;
        switch (call.getMethodId())
        {
           case MethodDeclarations.replicateMethod_id:
              fqnAsString = extractFqnFromMethodCall(call);
              break;
           case MethodDeclarations.replicateAllMethod_id:
              fqnAsString = extractFqnFromListOfMethodCall(call);
              break;
           case MethodDeclarations.dispatchRpcCallMethod_id:
              JBCMethodCall call2 = (JBCMethodCall) call.getArgs()[1];
              fqnAsString = extractFqn(call2);
              break;
           default:
              fqnAsString = extractFqn(call);
        }

        return fqnAsString;
    }

    // --------- Marshalling methods

    private void marshallObject(Object o, ObjectOutputStream out, Map refMap) throws Exception
    {
        if (o == null)
        {
            out.writeByte(MAGICNUMBER_NULL);
        }
        else if (refMap.containsKey(o)) // see if this object has been marshalled before.
        {
            out.writeByte(MAGICNUMBER_REF);
            out.writeShort(((Integer) refMap.get(o)).intValue());
        }
        else if (o instanceof JBCMethodCall)
        {
            // first see if this is a 'known' method call.
            JBCMethodCall call = (JBCMethodCall) o;

            if (call.getMethodId() > -1)
            {
                out.writeByte(MAGICNUMBER_METHODCALL);
                marshallMethodCall(call, out, refMap);
            }
            else
            {
                // treat this as a serializable object
//                if (log.isWarnEnabled()) log.warn("Treating method call " + call + " as a normal Serializable object, not attempting to marshall with method ids.");
//
//                int refId = createReference(o, refMap);
//                out.writeByte(MAGICNUMBER_SERIALIZABLE);
//                out.writeShort(refId);
//                out.writeObject(call);
                throw new IllegalArgumentException("MethodCall "+call+" does not have a valid method id.  Was this method call created with MethodCallFactory?");
            }
        }
        else if (o instanceof Fqn)
        {
            int refId = createReference(o, refMap);
            out.writeByte(MAGICNUMBER_FQN);
            out.writeShort(refId);
            marshallFqn((Fqn) o, out, refMap);
        }
        else if (o instanceof GlobalTransaction)
        {
            int refId = createReference(o, refMap);
            out.writeByte(MAGICNUMBER_GTX);
            out.writeShort(refId);
            marshallGlobalTransaction((GlobalTransaction) o, out, refMap);
        }
        else if (o instanceof IpAddress)
        {
            out.writeByte(MAGICNUMBER_IPADDRESS);
            marshallIpAddress((IpAddress) o, out);
        }
        else if (o.getClass().equals(ArrayList.class))
        {
            out.writeByte(MAGICNUMBER_ARRAY_LIST);
            marshallCollection((Collection) o, out, refMap);
        }
        else if (o.getClass().equals(LinkedList.class))
        {
            out.writeByte(MAGICNUMBER_LINKED_LIST);
            marshallCollection((Collection) o, out, refMap);
        }
        else if (o.getClass().equals(HashMap.class))
        {
            out.writeByte(MAGICNUMBER_HASH_MAP);
            marshallMap((Map) o, out, refMap);
        }
        else if (o.getClass().equals(TreeMap.class))
        {
            out.writeByte(MAGICNUMBER_TREE_MAP);
            marshallMap((Map) o, out, refMap);
        }
        else if (o.getClass().equals(HashSet.class))
        {
            out.writeByte(MAGICNUMBER_HASH_SET);
            marshallCollection((Collection) o, out, refMap);
        }
        else if (o.getClass().equals(TreeSet.class))
        {
            out.writeByte(MAGICNUMBER_TREE_SET);
            marshallCollection((Collection) o, out, refMap);
        }
        else if (o instanceof Boolean)
        {
            out.writeByte(MAGICNUMBER_BOOLEAN);
            out.writeBoolean(((Boolean) o).booleanValue());
        }
        else if (o instanceof Integer)
        {
            out.writeByte(MAGICNUMBER_INTEGER);
            out.writeInt(((Integer) o).intValue());
        }
        else if (o instanceof Long)
        {
            out.writeByte(MAGICNUMBER_LONG);
            out.writeLong(((Long) o).longValue());
        }
        else if (o instanceof String)
        {
            int refId = createReference(o, refMap);
            out.writeByte(MAGICNUMBER_STRING);
            out.writeShort(refId);
            out.writeUTF((String) o);
        }
        else if (o instanceof Serializable || ObjectSerializationFactory.useJBossSerialization())
        {
            int refId = createReference(o, refMap);
            if (log.isTraceEnabled()) log.trace("Warning: using object serialization for " + o.getClass());
            out.writeByte(MAGICNUMBER_SERIALIZABLE);
            out.writeShort(refId);
            out.writeObject(o);
        }
        else
        {
            throw new Exception("Don't know how to marshall object of type " + o.getClass());
        }
    }

    private int createReference(Object o, Map refMap)
    {
        int reference = refMap.size();
        refMap.put(o, new Integer(reference));
        return reference;
    }

    private void marshallMethodCall(JBCMethodCall methodCall, ObjectOutputStream out, Map refMap) throws Exception
    {
        out.writeShort(methodCall.getMethodId());
        Object[] args = methodCall.getArgs();
        byte numArgs = (byte) (args == null ? 0 : args.length);
        out.writeByte(numArgs);

        for (int i = 0; i < numArgs; i++)
        {
            marshallObject(args[i], out, refMap);
        }
    }

    private void marshallGlobalTransaction(GlobalTransaction globalTransaction, ObjectOutputStream out, Map refMap) throws Exception
    {
        out.writeLong(globalTransaction.getId());
        marshallObject(globalTransaction.getAddress(), out, refMap);
    }


    private void marshallFqn(Fqn fqn, ObjectOutputStream out, Map refMap) throws Exception
    {
        boolean isRoot = fqn.isRoot();
        out.writeBoolean(isRoot);
        if (!isRoot)
        {
            out.writeShort(fqn.size());
            for (int i = 0; i < fqn.size(); i++)
            {
                marshallObject(fqn.get(i), out, refMap);
            }
        }
    }

    private void marshallIpAddress(IpAddress ipAddress, ObjectOutputStream out) throws Exception
    {
        ipAddress.writeExternal(out);
    }

    private void marshallCollection(Collection c, ObjectOutputStream out, Map refMap) throws Exception
    {
        out.writeInt(c.size());
        Iterator i = c.iterator();
        while (i.hasNext())
        {
            marshallObject(i.next(), out, refMap);
        }
    }

   private void marshallMap(Map m, ObjectOutputStream out, Map refMap) throws Exception
   {
      out.writeInt(m.size());
      Iterator i = m.keySet().iterator();
      while (i.hasNext())
      {
         Object key = i.next();
         marshallObject(key, out, refMap);
         marshallObject(m.get(key), out, refMap);
      }
   }

    // --------- Unmarshalling methods

    private Object unmarshallObject(ObjectInputStream in, ClassLoader loader, Map refMap) throws Exception
    {
        if (loader == null)
        {
            return unmarshallObject(in, refMap);
        }
        else
        {
            Thread currentThread = Thread.currentThread();
            ClassLoader old = currentThread.getContextClassLoader();
            try
            {
                currentThread.setContextClassLoader(loader);
                return unmarshallObject(in, refMap);
            }
            finally
            {
                currentThread.setContextClassLoader(old);
            }
        }
    }

    private Object unmarshallObject(ObjectInputStream in, Map refMap) throws Exception
    {
        byte magicNumber = in.readByte();
        Integer reference;
        Object retVal;
        switch (magicNumber)
        {
            case MAGICNUMBER_NULL:
                return null;
            case MAGICNUMBER_REF:
                reference = new Integer(in.readShort());
                if (!refMap.containsKey(reference))
                {
                    throw new IOException("Unable to locate object reference " + reference + " in byte stream!");
                }
                return refMap.get(reference);
            case MAGICNUMBER_SERIALIZABLE:
                reference = new Integer(in.readShort());
                retVal = in.readObject();
                refMap.put(reference, retVal);
                return retVal;
            case MAGICNUMBER_METHODCALL:
                retVal = unmarshallMethodCall(in, refMap);
                return retVal;
            case MAGICNUMBER_FQN:
                reference = new Integer(in.readShort());
                retVal = unmarshallFqn(in, refMap);
                refMap.put(reference, retVal);
                return retVal;
            case MAGICNUMBER_GTX:
                reference = new Integer(in.readShort());
                retVal = unmarshallGlobalTransaction(in, refMap);
                refMap.put(reference, retVal);
                return retVal;
            case MAGICNUMBER_IPADDRESS:
                retVal = unmarshallIpAddress(in);
                return retVal;
            case MAGICNUMBER_ARRAY_LIST:
                return unmarshallArrayList(in, refMap);
            case MAGICNUMBER_LINKED_LIST:
                return unmarshallLinkedList(in, refMap);
            case MAGICNUMBER_HASH_MAP:
                return unmarshallHashMap(in, refMap);
            case MAGICNUMBER_TREE_MAP:
                return unmarshallTreeMap(in, refMap);
            case MAGICNUMBER_HASH_SET:
                return unmarshallHashSet(in, refMap);
            case MAGICNUMBER_TREE_SET:
                return unmarshallTreeSet(in, refMap);
            case MAGICNUMBER_BOOLEAN:
                return in.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
            case MAGICNUMBER_INTEGER:
                return new Integer(in.readInt());
            case MAGICNUMBER_LONG:
                retVal = new Long(in.readLong());
                return retVal;
            case MAGICNUMBER_STRING:
                reference = new Integer(in.readShort());
                retVal = in.readUTF();
                refMap.put(reference, retVal);
                return retVal;
            default:
                if (log.isErrorEnabled()) log.error("Unknown Magic Number " + magicNumber);
                throw new Exception("Unknown magic number " + magicNumber);
        }
    }

    private MethodCall unmarshallMethodCall(ObjectInputStream in, Map refMap) throws Exception
    {
        short methodId = in.readShort();
        byte numArgs = in.readByte();
        Object[] args = null;

        if (numArgs > 0)
        {
            args = new Object[numArgs];

            for (int i = 0; i < numArgs; i++)
            {
                args[i] = unmarshallObject(in, refMap);
            }
        }
        return MethodCallFactory.create(MethodDeclarations.lookupMethod(methodId), args);
    }

    private GlobalTransaction unmarshallGlobalTransaction(ObjectInputStream in, Map refMap) throws Exception
    {
        GlobalTransaction gtx = new GlobalTransaction();
        long id = in.readLong();
        Object address = unmarshallObject(in, refMap);
        gtx.setId(id);
        gtx.setAddress((Address) address);
        return gtx;
    }

    private Fqn unmarshallFqn(ObjectInputStream in, Map refMap) throws Exception
    {

        boolean isRoot = in.readBoolean();
        Fqn fqn;
        if (!isRoot)
        {
            int numElements = in.readShort();
            List elements = new ArrayList(numElements);
            for (int i = 0; i < numElements; i++)
            {
                elements.add(unmarshallObject(in, refMap));
            }
            fqn = new Fqn(elements);
        }
        else
        {
            fqn = Fqn.ROOT;
        }
        return fqn;
    }

    private IpAddress unmarshallIpAddress(ObjectInputStream in) throws Exception
    {
        IpAddress ipAddress = new IpAddress();
        ipAddress.readExternal(in);
        return ipAddress;
    }

    private List unmarshallArrayList(ObjectInputStream in, Map refMap) throws Exception
    {
        int listSize = in.readInt();
        List list = new ArrayList(listSize);
        for (int i = 0; i < listSize; i++)
        {
            list.add(unmarshallObject(in, refMap));
        }
        return list;
    }

   private List unmarshallLinkedList(ObjectInputStream in, Map refMap) throws Exception
    {
        int listSize = in.readInt();
        List list = new LinkedList();
        for (int i = 0; i < listSize; i++)
        {
            list.add(unmarshallObject(in, refMap));
        }
        return list;
    }

   private Map unmarshallHashMap(ObjectInputStream in, Map refMap) throws Exception
    {
        int listSize = in.readInt();
        Map map = new HashMap();
        for (int i = 0; i < listSize; i++)
        {
            map.put(unmarshallObject(in, refMap), unmarshallObject(in, refMap));
        }
        return map;
    }

   private Map unmarshallTreeMap(ObjectInputStream in, Map refMap) throws Exception
    {
        int listSize = in.readInt();
        Map map = new TreeMap();
        for (int i = 0; i < listSize; i++)
        {
            map.put(unmarshallObject(in, refMap), unmarshallObject(in, refMap));
        }
        return map;
    }

   private Set unmarshallHashSet(ObjectInputStream in, Map refMap) throws Exception
    {
        int listSize = in.readInt();
        Set map = new HashSet();
        for (int i = 0; i < listSize; i++)
        {
            map.add(unmarshallObject(in, refMap));
        }
        return map;
    }

   private Set unmarshallTreeSet(ObjectInputStream in, Map refMap) throws Exception
    {
        int listSize = in.readInt();
        Set map = new TreeSet();
        for (int i = 0; i < listSize; i++)
        {
            map.add(unmarshallObject(in, refMap));
        }
        return map;
    }


    class InactiveRegionException extends CacheException
    {
        public InactiveRegionException()
        {
            super();
        }

        public InactiveRegionException(String msg)
        {
            super(msg);
        }

        public InactiveRegionException(String msg, Throwable cause)
        {
            super(msg, cause);
        }
    }
}
