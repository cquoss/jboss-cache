package org.jboss.cache.marshall;

/**
 * Region name conflicts with pre-existing regions. The conflict may come from there is already an
 * parent region defined.
 *
 * @author Ben Wang 8-2005
 */
public class RegionNameConflictException extends Exception
{
    public RegionNameConflictException()
    {
        super();
    }

    public RegionNameConflictException(String msg)
    {
        super(msg);
    }

    public RegionNameConflictException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
