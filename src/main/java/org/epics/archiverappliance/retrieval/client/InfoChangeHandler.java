package org.epics.archiverappliance.retrieval.client;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;

/**
 * Handle info change events.
 * Events are returned older events first; so updates to metadata can come in later chunks.
 * @author mshankar
 *
 */
public interface InfoChangeHandler {
    void handleInfoChange(EPICSEvent.PayloadInfo info);
}
