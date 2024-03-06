/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Main interface for client retrieving data using the PB/HTTP protocol.
 * @author mshankar
 */
public abstract class DataRetrieval {
    /**
     * Get data for PV pvName from starttime to endtime. By default, we expect to get raw data.
     * @param pvName The name of the pv
     * @param startTime Start time of request
     * @param endTime End time of request
     * @return Return an iterator over the data.
     */
    public final GenMsgIterator getDataForPV(String pvName, Timestamp startTime, Timestamp endTime) {
        return getDataForPV(pvName, startTime, endTime, false);
    }
    ;
    /**
     * Get data for PV pvName from starttime to endtime using the system defined sparsification operator.
     * @param pvName The name of the pv
     * @param startTime Start time of request
     * @param endTime End time of request
     * @param useReducedDataSet - If true, use the server defined sparsification operator...
     * @return Return an iterator over the data.
     */
    public final GenMsgIterator getDataForPV(
            String pvName, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet) {
        return getDataForPV(pvName, startTime, endTime, useReducedDataSet, null);
    }

    /**
     * Get data for PV pvName from starttime to endtime using the system defined sparsification operator; pass additional params in the HTTP call.
     * @param pvName The name of the pv
     * @param startTime Start time of request
     * @param endTime End time of request
     * @param useReducedDataSet - If true, use the server defined sparsification operator...
     * @param otherParams - Any other name/value pairs that are passed onto the server.
     * @return Return an iterator over the data.
     */
    public final GenMsgIterator getDataForPV(
            String pvName,
            Timestamp startTime,
            Timestamp endTime,
            boolean useReducedDataSet,
            Map<String, String> otherParams) {
        return getDataForPVs(Collections.singletonList(pvName), startTime, endTime, useReducedDataSet, otherParams);
    }

    /**
     * Get data for PVs in pvNames from starttime to endtime using the system defined sparsification operator; pass additional params in the HTTP call.
     * NOTE: The data for all the PVs will be concatenated together. To identify which PV is currently being read, you must register your own
     * {@code InfoChangeHandler} to the returned {@code GenMsgIterator} using {@code onInfoChange}.
     * @param pvNames The names of the pvs
     * @param startTime Start time of request
     * @param endTime End time of request
     * @param useReducedDataSet - If true, use the server defined sparsification operator...
     * @param otherParams - Any other name/value pairs that are passed onto the server.
     * @return Return an iterator over the data.
     */
    public abstract GenMsgIterator getDataForPVs(
            List<String> pvNames,
            Timestamp startTime,
            Timestamp endTime,
            boolean useReducedDataSet,
            Map<String, String> otherParams);
}
