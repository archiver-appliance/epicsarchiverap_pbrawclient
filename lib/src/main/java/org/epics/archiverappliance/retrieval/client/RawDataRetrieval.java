/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import java.io.BufferedInputStream;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hc.client5.http.fluent.Request;

/**
 * Client side class for retrieving data from the appliance archiver using the PB over HTTP protocol.
 *
 * @author mshankar
 */
public class RawDataRetrieval implements DataRetrieval {
    private static final Logger logger = Logger.getLogger(RawDataRetrieval.class.getName());
    private final String accessURL;

    public RawDataRetrieval(String accessURL) {
        this.accessURL = accessURL;
    }

    private static String convertToUTC(Timestamp time) {
        return URLEncoder.encode(time.toInstant().toString(), StandardCharsets.UTF_8);
    }

    @Override
    public GenMsgIterator getDataForPV(String pvName, Timestamp startTime, Timestamp endTime) {
        return getDataForPV(pvName, startTime, endTime, false, null);
    }

    @Override
    public GenMsgIterator getDataForPV(
            String pvName, Timestamp startTime, Timestamp endTime, boolean useReducedDataSet) {
        return getDataForPV(pvName, startTime, endTime, useReducedDataSet, null);
    }

    @Override
    public GenMsgIterator getDataForPV(
            String pvName,
            Timestamp startTime,
            Timestamp endTime,
            boolean useReducedDataSet,
            Map<String, String> otherParams) {
        // We'll use java.net for now.
        StringWriter buf = new StringWriter();
        String encode = URLEncoder.encode(pvName, StandardCharsets.UTF_8);
        buf.append(accessURL);
        if (accessURL.contains("?")) {
            buf.append("&pv=").append(encode);
        } else {
            buf.append("?pv=").append(encode);
        }
        buf.append("&from=").append(convertToUTC(startTime)).append("&to=").append(convertToUTC(endTime));
        if (useReducedDataSet) {
            buf.append("&usereduced=true");
        }
        if (otherParams != null) {
            otherParams.keySet().forEach(key -> {
                buf.append("&");
                buf.append(key);
                buf.append("=");
                buf.append(otherParams.get(key));
            });
        }
        String getURL = buf.toString();
        logger.info("URL to fetch data is " + getURL);
        try {
            URL url = new URL(getURL);
            var data = Request.get(url.toURI()).execute().returnContent();

            BufferedInputStream is = new BufferedInputStream(data.asStream());

            if (is.available() <= 0) return null;
            return new InputStreamBackedGenMsg(is);

        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Exception fetching data from URL " + getURL, ex);
        }
        return null;
    }
}
