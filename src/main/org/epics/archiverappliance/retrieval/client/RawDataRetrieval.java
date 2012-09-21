/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;


import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

/**
 * Client side class for retrieving data from the appliance archiver using the PB over HTTP protocol.
 * @author mshankar
 *
 */
public class RawDataRetrieval implements DataRetrieval {
	private static Logger logger = Logger.getLogger(RawDataRetrieval.class.getName());
	private String accessURL = null;
	
	public RawDataRetrieval(String accessURL) {
		this.accessURL = accessURL;
	}

	@Override
	public GenMsgIterator getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime) {
		return getDataForPVS(pvNames, startTime, endTime, false, null);
	}

	@Override
	public GenMsgIterator getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet) {
		return getDataForPVS(pvNames, startTime, endTime, useReducedDataSet, null);
	}

	@Override
	public GenMsgIterator getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet, HashMap<String, String> otherParams) {
		StringWriter concatedPVs = new StringWriter();
		boolean isFirstEntry = true;
		for(String pvName : pvNames) {
			if(isFirstEntry) {
				isFirstEntry = false;
			} else {
				concatedPVs.append(",");
			}
			concatedPVs.append(pvName);
		}
		// We'll use java.net for now.
		StringWriter buf = new StringWriter();
		buf.append(accessURL)
		.append("?pv=").append(concatedPVs.toString())
		.append("&from=").append(convertToUTC(startTime))
		.append("&to=").append(convertToUTC(endTime));
		if(useReducedDataSet) {
			buf.append("&usereduced=true");
		}
		if(otherParams != null) {
			for(String key : otherParams.keySet()) {
				buf.append("&");
				buf.append(key);
				buf.append("=");
				buf.append(otherParams.get(key));
			}
		}
		String getURL = buf.toString();
		logger.info("URL to fetch data is " + getURL);
		try {
			URL url = new URL(getURL);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.connect();
			if(connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
				InputStream is = new BufferedInputStream(connection.getInputStream());
				if(is.available() <= 0) return null;
				return new InputStreamBackedGenMsg(is);
			} else { 
				logger.info("No data found for PVs " + concatedPVs + " + using URL " + url.toString());
				return null;
			}
		} catch(Exception ex) {
			logger.log(Level.SEVERE, "Exception fetching data from URL " + getURL, ex);
		}
		return null;
	}
	
	private static String convertToUTC(Timestamp time) { 
		Calendar c = GregorianCalendar.getInstance();
		c.setTime(time);
		return DatatypeConverter.printDateTime(c);		
	}	
}