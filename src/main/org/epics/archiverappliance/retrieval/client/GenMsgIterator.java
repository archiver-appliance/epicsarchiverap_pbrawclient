package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;

public interface GenMsgIterator extends Iterable<EpicsMessage> {
	public EPICSEvent.PayloadInfo getPayLoadInfo();
}
