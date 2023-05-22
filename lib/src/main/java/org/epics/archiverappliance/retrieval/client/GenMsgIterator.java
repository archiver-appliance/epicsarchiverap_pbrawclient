package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import java.io.Closeable;

/**
 * Similar to EventStream but much more lightweight...
 * @author mshankar
 *
 */
public interface GenMsgIterator extends Iterable<EpicsMessage>, Closeable {
    public EPICSEvent.PayloadInfo getPayLoadInfo();

    public void onInfoChange(InfoChangeHandler handler);
}
