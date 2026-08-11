/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadType;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

/**
 * Test retrieval for single PVs
 * @author mshankar
 *
 */
public class SinglePVRetrievalTest {

    /**
     * singleFileWithWellKnownPoints file with one data point per day for 2012. All data points are for 09:43:37 UTC.
     * @throws Exception
     */
    @Test
    void testSingleFileWithWellKnownPoints() throws Exception {
        try (FileInputStream fis = new FileInputStream("src/test/resources/sampledata/singleFileWithWellKnownPoints");
                InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
            int eventCount = 0;
            Timestamp previousTs = new Timestamp(0);
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
            Calendar expectedTime = Calendar.getInstance(timeZone);
            expectedTime.set(2012, 0, 1, 9, 43, 37);
            expectedTime.set(Calendar.MILLISECOND, 0);
            for (EpicsMessage msg : is) {
                Calendar actualTime = Calendar.getInstance(timeZone);
                Timestamp ts = msg.getTimestamp();
                assertTrue(
                        ts.getTime() >= previousTs.getTime(),
                        "Not monotonically increasing timestamps at event " + eventCount + " time " + ts.getTime()
                                + " and previous " + previousTs.getTime());
                actualTime.setTimeInMillis(ts.getTime());
                assertEquals(
                        0,
                        expectedTime.compareTo(actualTime),
                        "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                + format.format(actualTime.getTime()) + " at event " + eventCount);
                previousTs = ts;
                expectedTime.add(Calendar.HOUR, 24);
                eventCount++;
            }
            assertEquals(366, eventCount, "Event count is not what we expect. We got " + eventCount);
        }
    }

    /**
     * Test file with one data point per day for 2012; however, there is a header after each datapoint. All data points are for 09:43:37 UTC
     * @throws Exception
     */
    @Test
    void testMultipleChunksInSameYear() throws Exception {
        try (FileInputStream fis = new FileInputStream("src/test/resources/sampledata/multipleChunksInSameYear");
                InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
            int eventCount = 0;
            Timestamp previousTs = new Timestamp(0);
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
            Calendar expectedTime = Calendar.getInstance(timeZone);
            expectedTime.set(2012, 0, 1, 9, 43, 37);
            expectedTime.set(Calendar.MILLISECOND, 0);
            for (EpicsMessage msg : is) {
                Calendar actualTime = Calendar.getInstance(timeZone);
                Timestamp ts = msg.getTimestamp();
                assertTrue(
                        ts.getTime() >= previousTs.getTime(),
                        "Not monotonically increasing timestamps at event " + eventCount + " time " + ts.getTime()
                                + " and previous " + previousTs.getTime());
                actualTime.setTimeInMillis(ts.getTime());
                assertEquals(
                        0,
                        expectedTime.compareTo(actualTime),
                        "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                + format.format(actualTime.getTime()) + " at event " + eventCount);
                previousTs = ts;
                expectedTime.add(Calendar.HOUR, 24);
                eventCount++;
            }
            assertEquals(366, eventCount, "Event count is not what we expect. We got " + eventCount);
        }
    }

    /**
     * Test file with one data point per day for 2012; this is broken down into chunks of random sizes All data points are for 09:43:37 UTC.
     * @throws Exception
     */
    @Test
    void testMultipleChunksOfRandomSizeInSameYear() throws Exception {
        try (FileInputStream fis =
                        new FileInputStream("src/test/resources/sampledata/multipleChunksOfRandomSizeInSameYear");
                InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
            int eventCount = 0;
            Timestamp previousTs = new Timestamp(0);
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
            Calendar expectedTime = Calendar.getInstance(timeZone);
            expectedTime.set(2012, 0, 1, 9, 43, 37);
            expectedTime.set(Calendar.MILLISECOND, 0);
            for (EpicsMessage msg : is) {
                Calendar actualTime = Calendar.getInstance(timeZone);
                Timestamp ts = msg.getTimestamp();
                assertTrue(
                        ts.getTime() >= previousTs.getTime(),
                        "Not monotonically increasing timestamps at event " + eventCount + " time " + ts.getTime()
                                + " and previous " + previousTs.getTime());
                actualTime.setTimeInMillis(ts.getTime());
                assertEquals(
                        0,
                        expectedTime.compareTo(actualTime),
                        "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                + format.format(actualTime.getTime()) + " at event " + eventCount);
                previousTs = ts;
                expectedTime.add(Calendar.HOUR, 24);
                eventCount++;
            }
            assertEquals(366, eventCount, "Event count is not what we expect. We got " + eventCount);
        }
    }

    /**
     * Test file with one data point per day from 1970-1970+2000.
     * @throws Exception
     */
    @Test
    void testMultipleChunksInMultipleYears() throws Exception {
        try (FileInputStream fis = new FileInputStream("src/test/resources/sampledata/multipleChunksInMultipleYears");
                InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
            int eventCountInYear = 0;
            int totalEventCount = 0;
            Timestamp previousTs = new Timestamp(0);
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
            Calendar expectedTime = Calendar.getInstance(timeZone);
            int year = 1970;
            expectedTime.set(year++, 0, 1, 9, 43, 37);
            expectedTime.set(Calendar.MILLISECOND, 0);
            for (EpicsMessage msg : is) {
                Calendar actualTime = Calendar.getInstance(timeZone);
                Timestamp ts = msg.getTimestamp();
                assertTrue(
                        ts.getTime() >= previousTs.getTime(),
                        "Not monotonically increasing timestamps at event " + eventCountInYear + " time " + ts.getTime()
                                + " and previous " + previousTs.getTime());
                actualTime.setTimeInMillis(ts.getTime());
                assertEquals(
                        0,
                        expectedTime.compareTo(actualTime),
                        "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                + format.format(actualTime.getTime()) + " at event " + eventCountInYear);
                previousTs = ts;
                expectedTime.add(Calendar.HOUR, 24);
                eventCountInYear++;
                totalEventCount++;
                if (eventCountInYear == 365) {
                    eventCountInYear = 0;
                    expectedTime.set(year++, 0, 1, 9, 43, 37);
                    expectedTime.set(Calendar.MILLISECOND, 0);
                }
            }
            assertEquals(
                    365 * 2000, totalEventCount, "Event count is not what we expect. We got " + totalEventCount);
        }
    }

    /**
     * Test file with some data points for 2012 for each DBR type. All data points are for 09:43:37 UTC.
     * @throws Exception
     */
    @Test
    void testFilesForDBRTypes() throws Exception {
        for (PayloadType payloadType : PayloadType.values()) {
            try (FileInputStream fis =
                            new FileInputStream("src/test/resources/sampledata/" + payloadType + "_sampledata");
                    InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
                int eventCount = 0;
                int expectedEventCount = 366;
                if (payloadType.getNumber() >= 7) expectedEventCount = 2;
                Timestamp previousTs = new Timestamp(0);
                TimeZone timeZone = TimeZone.getTimeZone("UTC");
                SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
                Calendar expectedTime = Calendar.getInstance(timeZone);
                expectedTime.set(2012, 0, 1, 9, 43, 37);
                expectedTime.set(Calendar.MILLISECOND, 0);
                for (EpicsMessage msg : is) {
                    Calendar actualTime = Calendar.getInstance(timeZone);
                    Timestamp ts = msg.getTimestamp();
                    assertTrue(
                            ts.getTime() >= previousTs.getTime(),
                            "Not monotonically increasing timestamps at event " + eventCount + " time " + ts.getTime()
                                    + " and previous " + previousTs.getTime());
                    actualTime.setTimeInMillis(ts.getTime());
                    assertEquals(
                            0,
                            expectedTime.compareTo(actualTime),
                            "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                    + format.format(actualTime.getTime()) + " at event " + eventCount);
                    previousTs = ts;
                    expectedTime.add(Calendar.HOUR, 24);
                    eventCount++;
                }
                assertEquals(
                        expectedEventCount,
                        eventCount,
                        "Event count is not what we expect. We got " + eventCount + " for " + payloadType);
            }
        }
    }

    /**
     * Test a days worth of data.
     * @throws Exception
     */
    @Test
    void testOneDaysWorthOfDBRDoubleData() throws Exception {
        try (FileInputStream fis = new FileInputStream("src/test/resources/sampledata/onedaysdbrdouble");
                InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
            int eventCount = 0;
            Timestamp previousTs = new Timestamp(0);
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
            Calendar expectedTime = Calendar.getInstance(timeZone);
            expectedTime.set(2011, 1, 1, 0, 0, 0);
            expectedTime.set(Calendar.MILLISECOND, 0);
            long startMillis = System.currentTimeMillis();
            for (EpicsMessage msg : is) {
                Calendar actualTime = Calendar.getInstance(timeZone);
                Timestamp ts = msg.getTimestamp();
                assertTrue(
                        ts.getTime() >= previousTs.getTime(),
                        "Not monotonically increasing timestamps at event " + eventCount + " time " + ts.getTime()
                                + " and previous " + previousTs.getTime());
                actualTime.setTimeInMillis(ts.getTime());
                if (expectedTime.compareTo(actualTime) != 0) {
                    assertEquals(
                            0,
                            expectedTime.compareTo(actualTime),
                            "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                    + format.format(actualTime.getTime()) + " at event " + eventCount);
                }
                previousTs = ts;
                expectedTime.add(Calendar.SECOND, 1);
                eventCount++;
            }
            assertEquals(86400, eventCount, "Event count is not what we expect. We got " + eventCount);
            long endMillis = System.currentTimeMillis();
            System.err.println("Time taken to process on days worth of data is " + (endMillis - startMillis) + "(ms)");
        }
    }
}
