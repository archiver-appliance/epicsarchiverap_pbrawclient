package org.epics.archiverappliance.retrieval.client;

import java.sql.Timestamp;

public class CheckArchApplPing {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String serverURL = "http://cdlx27.slac.stanford.edu:17665/retrieval";
		if(args.length > 1) { 
			serverURL = args[0];
		}
		String archApplPingPV = "ArchApplPingPV";

		Timestamp start = new Timestamp(System.currentTimeMillis()-1000);
		Timestamp end = new Timestamp(System.currentTimeMillis());
		boolean useReducedDataset = false;
		RawDataRetrieval rawDataRetrieval = new RawDataRetrieval(serverURL + "/data/getData.raw");
		GenMsgIterator strm = rawDataRetrieval.getDataForPV(archApplPingPV, start, end, useReducedDataset);
		long totalValues = 0;
		if(strm != null) {
			try {
				for(EpicsMessage dbrevent : strm) {
					dbrevent.getTimestamp();
					totalValues++;
				}
			} finally {
				strm.close();
			}
			if(totalValues == 0) { 
				System.err.println("We got an empty stream for the ping PV " + archApplPingPV);
			}
		} else {
			System.err.println("We got an empty stream for the ping PV " + archApplPingPV);
		}
	}
}
