package org.example;


import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import static org.example.Flight.INPUTCOLS.DEP_DELAY;
import static org.example.Flight.INPUTCOLS.FL_DATE;
import static org.junit.Assert.*;

public class TestFlight {

    private String validInput ;
    private String invalidFieldLenghtInput ;
    private String flightDivertedInput;
    private String flightCancelledInput;
    private String flightLateArrivalInput;

    @Before
    public void Fixtures() {
        this.validInput = "2018-01-02,AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,-9.00,0.00,,0.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-03 03:32:00";
        this.invalidFieldLenghtInput = "AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,-9.00,0.00,,0.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-03 03:32:00";
        this.flightDivertedInput = "2018-01-02,AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,-9.00,0.00,,1.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-03 03:32:00";
        this.flightCancelledInput = "2018-01-02,AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,-9.00,1.00,,0.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-03 03:32:00";
        this.flightLateArrivalInput = "2018-01-02,AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,15.00,0.00,,0.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-03 03:32:00";
    }

    @Test
    public void AssertTrue() {
        assertTrue(true);
    }

    @Test
    public void TestFromCsvValidFields() {
        Flight f = Flight.fromCsv(this.validInput);
        assertNotNull(f);
        assertEquals(f.getFields().length, f.getValidFieldNum()); ;
    }

    @Test
    public void TestFromCsvInvalidFields() {
        Flight f = Flight.fromCsv(this.invalidFieldLenghtInput);
        assertNull(f);
    }

    @Test
    public void TestGetField() {
        Flight f = Flight.fromCsv(this.validInput);
        assertEquals("2018-01-02", f.getField(FL_DATE));
    }

    @Test
    public void TestFlightIsNotDiverted() {
        Flight f = Flight.fromCsv(this.validInput);
        assertTrue(f.isNotDiverted());
    }

    @Test
    public void TestFlightIsDiverted() {
        Flight f = Flight.fromCsv(this.flightDivertedInput);
        assertFalse(f.isNotDiverted());
    }

    @Test
    public void TestFlightIsNotCancelled(){
        Flight f = Flight.fromCsv(this.validInput);
        assertTrue(f.isNotCancelled());
    }
    @Test
    public void TestFlightIsCancelled() {
        Flight f = Flight.fromCsv(this.flightCancelledInput);
        assertFalse(f.isNotCancelled());
    }

    @Test
    public void TestGetInputFeatures() {
        Flight f = Flight.fromCsv(this.validInput);
        float[] expectedFeatures = {-4f, 37f, 919f, Float.NaN, Float.NaN};
        float[] actualFeatures = f.getInputFeatures();
        assertArrayEquals(expectedFeatures, actualFeatures, 0.001f);
    }

    @Test
    public void TestToTrainingCsvOnTime() {
        String[] expectedTrainingCsv = {"1.0","-4.0","37.0","919.0","NaN","NaN","AA","25.79527778","-80.29000000","38.85138889","-77.03777778","MIA","DCA"};
        Flight f = Flight.fromCsv(this.validInput);
        String trainingCsv = f.toTrainingCsv();
        assertEquals(String.join(",", expectedTrainingCsv), trainingCsv);
    }

    @Test
    public void TestToTrainingCsvLateArrival() {
        String[] expectedTrainingCsv = {"0.0","-4.0","37.0","919.0","NaN","NaN","AA","25.79527778","-80.29000000","38.85138889","-77.03777778","MIA","DCA"};
        Flight f = Flight.fromCsv(this.flightLateArrivalInput);
        String trainingCsv = f.toTrainingCsv();
        assertEquals(String.join(",", expectedTrainingCsv), trainingCsv);
    }

    @Test
    public void TestGetDepartureHour() {
        // Actual departure time 1/3/2018 0:56:00 minus departure airport tz offset (-18000 sec)
        String expectedHour = "19";
        Flight f = Flight.fromCsv(this.validInput);
        assertEquals(expectedHour, f.getDepartureHour());
    }

    @Test
    public void TestGetFieldsAsFloat() {
        Float expectedDepDelay = -4.0f;
        Flight f = Flight.fromCsv(this.validInput);
        assertEquals(expectedDepDelay, f.getFieldAsFloat(DEP_DELAY));
    }

    @Test
    public void TestGetFieldAsFloatThrowNullPointerException() {
        Flight f = Flight.fromCsv(this.validInput);
        assertThrows(NullPointerException.class, () -> f.getFieldAsFloat(null));
    }

    @Test
    public void TestGetEventTimestamp() {
        Flight f = Flight.fromCsv(this.validInput);
        String expected = "2018-01-03 03:32:00";
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        assertEquals(expected, fmt.print(f.getEventTimestamp()));
    }
}
