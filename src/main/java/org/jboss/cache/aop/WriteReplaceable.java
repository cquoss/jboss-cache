/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.cache.aop;

import java.io.ObjectStreamException;

/**
 * @author <a href="mailto:harald@gliebe.de">Harald Gliebe</a>
 * @deprecated 1.0
 */

public interface WriteReplaceable
{

   Object writeReplace() throws ObjectStreamException;

}
