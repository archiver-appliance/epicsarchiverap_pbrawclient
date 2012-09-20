/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import java.io.Reader;
import java.sql.Timestamp;
import java.util.HashMap;

/**
 * Main interface for client retrieving data using the PB/HTTP protocol.
 * @author mshankar
 * @see DataRetrieval
 * @see Reader
 */
public interface DataRetrieval {
	/**
	 * Get data for PV from starttime to endtime.
	 * @param pvNames
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public GenMsgIterator getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime);
	/**
	 * Get data for PV from starttime to endtime using the system defined sparsification operator.
	 * @param pvNames
	 * @param startTime
	 * @param endTime
	 * @param useReducedDataSet - Use the server defined sparsification operator...
	 * @return
	 */
	public GenMsgIterator getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet);

	/**
	 * Get data for PV from starttime to endtime using the system defined sparsification operator; pass additional params in the HTTP call.
	 * @param pvNames
	 * @param startTime
	 * @param endTime
	 * @param useReducedDataSet - Use the server defined sparsification operator...
	 * @param otherParams - Any other name/value pairs that are passed onto the server. 
	 * @return
	 */
	public GenMsgIterator getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet, HashMap<String, String> otherParams);
}
