package org.epics.archiverappliance.retrieval.client;

import java.io.Closeable;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;

public interface GenMsgIterator extends Iterable<EpicsMessage>, Closeable {
	public EPICSEvent.PayloadInfo getPayLoadInfo();
}
