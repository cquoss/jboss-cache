package org.jboss.cache.marshall;

/**
 * Region name not found.
 *
 * @author Ben Wang 8-2005
 */
public class RegionNotFoundException extends Exception
{
    public RegionNotFoundException()
    {
        super();
    }

    public RegionNotFoundException(String msg)
    {
        super(msg);
    }

    public RegionNotFoundException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
