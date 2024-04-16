/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client side class for retrieving data from the appliance archiver using the PB over HTTP protocol.
 *
 * @author mshankar
 */
public class RawDataRetrieval extends DataRetrieval {
    private static final Logger logger = Logger.getLogger(RawDataRetrieval.class.getName());
    private final String accessURL;
    private HttpClient theClient;

    public RawDataRetrieval(String accessURL) {
        this.accessURL = accessURL;
        this.theClient =
                HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();
    }

    private static String convertToUTC(Timestamp time) {
        return URLEncoder.encode(time.toInstant().toString(), StandardCharsets.UTF_8);
    }

    @Override
    public final GenMsgIterator getDataForPVs(
            List<String> pvNames,
            Timestamp startTime,
            Timestamp endTime,
            boolean useReducedDataSet,
            Map<String, String> otherParams) {
        // We'll use java.net for now.
        StringWriter buf = new StringWriter();
        buf.append(accessURL);
        // If the access url has no query parameters then start new query, else append to existing
        var first_encoded = URLEncoder.encode(pvNames.get(0), StandardCharsets.UTF_8);
        if (accessURL.contains("?")) {
            buf.append("&pv=").append(first_encoded);
        } else {
            buf.append("?pv=").append(first_encoded);
        }
        for (var pvName : pvNames) {
            buf.append("&pv=").append(URLEncoder.encode(pvName, StandardCharsets.UTF_8));
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
            HttpRequest request =
                    HttpRequest.newBuilder().uri(URI.create(getURL)).build();
            HttpResponse<InputStream> response = this.theClient.send(request, BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                logger.warning("Invalid status code from server " + response.statusCode() + " when fetching data from "
                        + getURL);
                return null;
            }
            return new InputStreamBackedGenMsg(response.body());

        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Exception fetching data from URL " + getURL, ex);
        }
        return null;
    }
}
