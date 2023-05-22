/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Main interface for client retrieving data using the PB/HTTP protocol.
 * @author mshankar
 */
public interface DataRetrieval {
    /**
     * Get data for PV pvName from starttime to endtime. By default, we expect to get raw data.
     * @param pvName The name of the pv
     * @param startTime Start time of request
     * @param endTime End time of request
     * @return Return an iterator over the data.
     */
    public GenMsgIterator getDataForPV(String pvName, Timestamp startTime, Timestamp endTime);
    /**
     * Get data for PV pvName from starttime to endtime using the system defined sparsification operator.
     * @param pvName The name of the pv
     * @param startTime Start time of request
     * @param endTime End time of request
     * @param useReducedDataSet - If true, use the server defined sparsification operator...
     * @return Return an iterator over the data.
     */
    public GenMsgIterator getDataForPV(
            String pvName, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet);

    /**
     * Get data for PV pvName from starttime to endtime using the system defined sparsification operator; pass additional params in the HTTP call.
     * @param pvName The name of the pv
     * @param startTime Start time of request
     * @param endTime End time of request
     * @param useReducedDataSet - If true, use the server defined sparsification operator...
     * @param otherParams - Any other name/value pairs that are passed onto the server.
     * @return Return an iterator over the data.
     */
    public GenMsgIterator getDataForPV(
            String pvName,
            Timestamp startTime,
            Timestamp endTime,
            boolean useReducedDataSet,
            Map<String, String> otherParams);
}
