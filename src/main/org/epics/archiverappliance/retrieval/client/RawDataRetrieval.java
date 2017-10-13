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
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
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
	public GenMsgIterator getDataForPV(String pvName, Timestamp startTime, Timestamp endTime) {
		return getDataForPV(pvName, startTime, endTime, false, null);
	}

	@Override
	public GenMsgIterator getDataForPV(String pvName, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet) {
		return getDataForPV(pvName, startTime, endTime, useReducedDataSet, null);
	}

	@Override
	public GenMsgIterator getDataForPV(String pvName, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet, HashMap<String, String> otherParams) {
		// We'll use java.net for now.
		StringWriter buf = new StringWriter();
		String encode = pvName;
		try { 
			encode=URLEncoder.encode(pvName, "UTF-8");
		} catch (UnsupportedEncodingException ex) { 
			encode=pvName;
		}		
		buf.append(accessURL);
		if(accessURL.contains("?")) {
			buf.append("&pv=").append(encode);	
		} else {
			buf.append("?pv=").append(encode);
		}		
		buf.append("&from=").append(convertToUTC(startTime))
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
				logger.info("No data found for PV " + pvName + " + using URL " + url.toString());
				return null;
			}
		} catch(Exception ex) {
			logger.log(Level.SEVERE, "Exception fetching data from URL " + getURL, ex);
		}
		return null;
	}
	
	private static String convertToUTC(Timestamp time) {
		try { 
			Calendar c = GregorianCalendar.getInstance();
			c.setTime(time);
			return URLEncoder.encode(DatatypeConverter.printDateTime(c), "UTF-8");		
		} catch (UnsupportedEncodingException ex) {
			logger.log(Level.SEVERE, "Cannot encode times into UTF-8", ex);
			return null;
		}		
	}	
}