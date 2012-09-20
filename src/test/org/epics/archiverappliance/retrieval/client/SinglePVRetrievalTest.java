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

	@Test
	public void testSimpleGetData() throws Exception {
		try(FileInputStream fis = new FileInputStream("src/test/org/epics/archiverappliance/retrieval/client/sine12012.pb"); InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
			int eventCount = 0;
			Timestamp previousTs = new Timestamp(0);
			for(EpicsMessage msg : is) { 
				Timestamp ts = msg.getTimestamp();
				assertTrue("Not monotonically increasing timestamps at event " + eventCount + " time " + ts.getTime() + " and previous " + previousTs.getTime(), ts.getTime() >= previousTs.getTime());
				previousTs = ts;
				eventCount++;
			}
			assertTrue("Event count is not what we expect. We got " + eventCount, eventCount > 10);
		}
		
	}	
}
