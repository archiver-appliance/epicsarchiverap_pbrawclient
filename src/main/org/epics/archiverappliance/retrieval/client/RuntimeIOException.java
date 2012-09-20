package org.epics.archiverappliance.retrieval.client;

import java.io.IOException;

public class RuntimeIOException extends RuntimeException {
	private static final long serialVersionUID = -5720516062453402104L;

	public RuntimeIOException(String msg, IOException ex) { 
		super(msg, ex);
	}
}
