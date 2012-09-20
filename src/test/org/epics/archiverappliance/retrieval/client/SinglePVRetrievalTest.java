/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;


import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test retrieval for single PVs
 * @author mshankar
 *
 */
public class SinglePVRetrievalTest {
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	/**
	 * singleFileWithWellKnownPoints file with one data point per day for 2012. All data points are for 09:43:37 UTC.
	 * @throws Exception
	 */
	@Test
	public void testSingleFileWithWellKnownPoints() throws Exception {
		try(FileInputStream fis = new FileInputStream("src/test/org/epics/archiverappliance/retrieval/client/sampledata/singleFileWithWellKnownPoints"); InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
			int eventCount = 0;
			Timestamp previousTs = new Timestamp(0);
			TimeZone timeZone = TimeZone.getTimeZone("UTC");
			SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
			Calendar expectedTime = Calendar.getInstance(timeZone);
			expectedTime.set(2012, 0, 1, 9, 43, 37);
			expectedTime.set(Calendar.MILLISECOND, 0);
			for(EpicsMessage msg : is) {
				Calendar actualTime = Calendar.getInstance(timeZone);
				Timestamp ts = msg.getTimestamp();
				assertTrue("Not monotonically increasing timestamps at event " + eventCount + " time " + ts.getTime() + " and previous " + previousTs.getTime(), ts.getTime() >= previousTs.getTime());
				actualTime.setTimeInMillis(ts.getTime());
				assertTrue("Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is " + format.format(actualTime.getTime()) + " at event " + eventCount, expectedTime.compareTo(actualTime) == 0);
				previousTs = ts;
				expectedTime.add(Calendar.HOUR, 24);
				eventCount++;
			}
			assertTrue("Event count is not what we expect. We got " + eventCount, eventCount > 10);
		}
		
	}	
}
