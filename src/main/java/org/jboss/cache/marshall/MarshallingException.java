package org.jboss.cache.marshall;

/**
 * Thrown when there is an exception in marshalling.
 *
 * @author Ben Wang 8-2005
 */
public class MarshallingException extends Exception
{
    public MarshallingException()
    {
        super();
    }

    public MarshallingException(String msg)
    {
        super(msg);
    }

    public MarshallingException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
