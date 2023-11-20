package org.epics.archiverappliance.retrieval.client;

import java.io.IOException;
import java.io.Serial;

/**
 * Iterators do not let you throw Exceptions and the like. This is a hack to get around this limitation in Java.
 * @author mshankar
 *
 */
public class RuntimeIOException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -5720516062453402104L;

    public RuntimeIOException(String msg, IOException ex) {
        super(msg, ex);
    }
}
