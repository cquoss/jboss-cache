/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.cache.buddyreplication;

import org.jgroups.stack.IpAddress;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

/**
 * Value object that represents a buddy group
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 */
public class BuddyGroup implements Serializable //Externalizable
{
    private String groupName;
    private IpAddress dataOwner;
    /**
     * Vector<Address> - a list of JGroups addresses
     */
    List buddies = Collections.synchronizedList(new ArrayList());
//    List buddies = new ArrayList();

    public String getGroupName()
    {
        return groupName;
    }

    public void setGroupName(String groupName)
    {
        this.groupName = groupName;
    }

    public IpAddress getDataOwner()
    {
        return dataOwner;
    }

    public void setDataOwner(IpAddress dataOwner)
    {
        this.dataOwner = dataOwner;
    }

    public List getBuddies()
    {
        return buddies;
    }

    public void setBuddies(List buddies)
    {
        this.buddies = buddies;
    }

//    public void writeExternal(ObjectOutput out) throws IOException
//    {
//        out.writeObject(groupName);
//        out.writeObject(dataOwner);
//        out.writeObject(buddies);
//    }
//
//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
//    {
//        groupName = (String) in.readObject();
//        dataOwner = (IpAddress) in.readObject();
//        buddies = (List) in.readObject();
//    }

    public String toString()
    {
        StringBuffer b = new StringBuffer("BuddyGroup: (");
        b.append("dataOwner: ").append(dataOwner).append(", ");
        b.append("groupName: ").append(groupName).append(", ");
        b.append("buddies: ").append(buddies).append(")");
        return b.toString();
    }

}
