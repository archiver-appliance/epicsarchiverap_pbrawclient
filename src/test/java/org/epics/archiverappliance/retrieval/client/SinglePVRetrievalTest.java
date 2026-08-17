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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test retrieval for single PVs
 * @author mshankar
 *
 */
public class SinglePVRetrievalTest {

    private static final String SAMPLE_DATA_DIR = "src/test/resources/sampledata/";

    /**
     * The same day's worth of data laid out three different ways in the stream; one data point per day
     * for 2012, all at 09:43:37 UTC.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource({
        "singleFileWithWellKnownPoints, 366",
        "multipleChunksInSameYear, 366",
        "multipleChunksOfRandomSizeInSameYear, 366",
    })
    void chunkLayouts(String fixture, int expectedEventCount) throws Exception {
        assertSampleFile(fixture, expectedEventCount, utc(2012, 0, 1, 9, 43, 37), Calendar.HOUR, 24);
    }

    /**
     * A file with some data points for 2012 for each DBR type. All data points are for 09:43:37 UTC.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(PayloadType.class)
    void filesForDBRTypes(PayloadType payloadType) throws Exception {
        int expectedEventCount = payloadType.getNumber() >= 7 ? 2 : 366;
        assertSampleFile(
                payloadType + "_sampledata", expectedEventCount, utc(2012, 0, 1, 9, 43, 37), Calendar.HOUR, 24);
    }

    /**
     * Test a days worth of data.
     */
    @Test
    void oneDaysWorthOfDBRDoubleData() throws Exception {
        assertSampleFile("onedaysdbrdouble", 86400, utc(2011, 1, 1, 0, 0, 0), Calendar.SECOND, 1);
    }

    /**
     * Test file with one data point per day from 1970-1970+2000. Kept separate from the other fixtures as the
     * expected time restarts at the top of each year rather than advancing continuously.
     */
    @Test
    void multipleChunksInMultipleYears() throws Exception {
        try (FileInputStream fis = new FileInputStream(SAMPLE_DATA_DIR + "multipleChunksInMultipleYears");
                InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
            int eventCountInYear = 0;
            int totalEventCount = 0;
            Timestamp previousTs = new Timestamp(0);
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
            Calendar expectedTime = utc(1970, 0, 1, 9, 43, 37);
            int year = 1971;
            for (EpicsMessage msg : is) {
                Calendar actualTime = Calendar.getInstance(timeZone);
                Timestamp ts = msg.getTimestamp();
                Timestamp previous = previousTs;
                int eventIndex = eventCountInYear;
                actualTime.setTimeInMillis(ts.getTime());
                assertTrue(
                        ts.getTime() >= previous.getTime(),
                        () -> "Not monotonically increasing timestamps at event " + eventIndex + " time " + ts.getTime()
                                + " and previous " + previous.getTime());
                assertEquals(
                        0,
                        expectedTime.compareTo(actualTime),
                        () -> "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                + format.format(actualTime.getTime()) + " at event " + eventIndex);
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
                    365 * 2000,
                    totalEventCount,
                    "Event count is not what we expect. We got " + totalEventCount);
        }
    }

    /**
     * Walk a sample file, checking that timestamps increase monotonically and land on the expected time, and that
     * the file holds the expected number of events.
     *
     * @param fixture Name of the file under the sample data directory
     * @param expectedEventCount Number of events the file is expected to hold
     * @param expectedTime Time of the first event; advanced by stepAmount/stepField for each subsequent event
     * @param stepField The {@code Calendar} field the expected time advances by
     * @param stepAmount How much stepField advances by for each event
     */
    private void assertSampleFile(
            String fixture, int expectedEventCount, Calendar expectedTime, int stepField, int stepAmount)
            throws Exception {
        try (FileInputStream fis = new FileInputStream(SAMPLE_DATA_DIR + fixture);
                InputStreamBackedGenMsg is = new InputStreamBackedGenMsg(fis)) {
            int eventCount = 0;
            Timestamp previousTs = new Timestamp(0);
            TimeZone timeZone = TimeZone.getTimeZone("UTC");
            SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss.SSS Z");
            for (EpicsMessage msg : is) {
                Calendar actualTime = Calendar.getInstance(timeZone);
                Timestamp ts = msg.getTimestamp();
                Timestamp previous = previousTs;
                int eventIndex = eventCount;
                actualTime.setTimeInMillis(ts.getTime());
                assertTrue(
                        ts.getTime() >= previous.getTime(),
                        () -> "Not monotonically increasing timestamps at event " + eventIndex + " time " + ts.getTime()
                                + " and previous " + previous.getTime());
                assertEquals(
                        0,
                        expectedTime.compareTo(actualTime),
                        () -> "Expecting time to be " + format.format(expectedTime.getTime()) + " instead it is "
                                + format.format(actualTime.getTime()) + " at event " + eventIndex);
                previousTs = ts;
                expectedTime.add(stepField, stepAmount);
                eventCount++;
            }
            assertEquals(
                    expectedEventCount,
                    eventCount,
                    "Event count is not what we expect. We got " + eventCount + " for " + fixture);
        }
    }

    private static Calendar utc(int year, int month, int day, int hour, int minute, int second) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.set(year, month, day, hour, minute, second);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }
}
