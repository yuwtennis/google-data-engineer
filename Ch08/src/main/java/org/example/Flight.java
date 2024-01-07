package org.example;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@DefaultCoder(AvroCoder.class)
public class Flight {

    private static final Logger LOG = LoggerFactory.getLogger(Flight.class);

    private enum INPUTCOLS {
        FL_DATE,OP_UNIQUE_CARRIER,OP_CARRIER_AIRLINE_ID,OP_CARRIER,OP_CARRIER_FL_NUM,
        ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,
        DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,
        WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,
        DIVERTED,DISTANCE,DEP_AIRPORT_LAT,DEP_AIRPORT_LON,DEP_AIRPORT_TZOFFSET,ARR_AIRPORT_LAT,
        ARR_AIRPORT_LON,ARR_AIRPORT_TZOFFSET,EVENT,NOTIFY_TIME
    }

    private String[] fields;
    private float avgDepartureDelay, avgArrivalDelay;

    /**
     * Creates a Flight object from the given CSV line.
     *
     * @param line The CSV line representing the flight data.
     * @return The Flight object created from the CSV line. Returns null if the CSV line is malformed.
     */
    public static Flight fromCsv(String line) {
        Flight f = new Flight();
        f.fields = line.split(",");
        f.avgArrivalDelay = f.avgDepartureDelay = Float.NaN;
        if (f.fields.length == INPUTCOLS.values().length) {
            return f;
        }
        return null ; // malformed
    }

    /**
     * Checks if the flight is not diverted.
     *
     * @return true if the flight is not diverted, false otherwise.
     */
    public boolean isNotDiverted() {
        return fields[INPUTCOLS.DIVERTED.ordinal()].equals("0.00");
    }

    /**
     * Checks if the flight is not cancelled.
     *
     * @return true if the flight is not cancelled, false otherwise.
     */
    public boolean isNotCancelled() {
        return fields[INPUTCOLS.CANCELLED.ordinal()].equals("0.00");
    }

    /**
     * Determines if the event is an arrival event.
     *
     * @return true if the event is an arrival event, false otherwise
     */
    public boolean isArrivedEvent() {
        return fields[INPUTCOLS.EVENT.ordinal()].equals("arrived");
    }

    /**
     * Retrieves the input features for a given data record.
     * These features are extracted from the fields of the record and returned as an array of floats.
     *
     * @return an array of floats representing the input features
     */
    public float[] getInputFeatures() {
        float[] result = new float[5];
        int col = 0;
        result[col++] = Float.parseFloat(fields[INPUTCOLS.DEP_DELAY.ordinal()]);
        result[col++] = Float.parseFloat(fields[INPUTCOLS.TAXI_OUT.ordinal()]);
        result[col++] = Float.parseFloat(fields[INPUTCOLS.DISTANCE.ordinal()]);
        result[col++] = avgDepartureDelay;
        result[col++] = avgArrivalDelay;
        return result;
    }

    /**
     * Converts the flight data into a comma-separated value (CSV) string in the format required for training.
     * The CSV string represents the input features and the target variable (ontime).
     *
     * @return The flight data as a CSV string.
     */
    public String toTrainingCsv() {
        float[] features = this.getInputFeatures();
        float arrivalDelay = Float.parseFloat(fields[INPUTCOLS.ARR_DELAY.ordinal()]);
        boolean ontime = arrivalDelay < 15 ;
        StringBuilder sb = new StringBuilder();
        sb.append(ontime ? 1.0 : 0.0);
        sb.append(",");
        for (int i = 0; i < features.length; ++i) {
            sb.append(features[i]);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1); // last comma
        return sb.toString();
    }

    /**
     * Retrieves the list of fields for the current object.
     *
     * @return an array of Strings that represents the fields.
     */
    public String[] getFields() {
        return fields;
    }

    public String getField(String fieldName) throws IllegalArgumentException {
        return fields[INPUTCOLS.valueOf(fieldName).ordinal()] ;
    }

    /**
     * Retrieves the average departure delay of a flight.
     *
     * @return The average departure delay as a float.
     */
    public float getAvgDepartureDelay() {
        return avgDepartureDelay;
    }

    /**
     * Retrieves the average arrival delay of a flight.
     *
     * @return The average arrival delay as a float.
     */
    public float getAvgArrivalDelay() {
        return avgArrivalDelay;
    }

    /**
     * Retrieves the number of valid fields.
     *
     * @return The number of valid fields.
     */
    public int getValidFieldNum() {
        return INPUTCOLS.values().length;
    }

    /**
     * Retrieves the hour of departure for a flight.
     *
     * @return The hour of departure as a string.
     */
    public String getDepartureHour() {
        String format = "yyyy-MM-dd HH:mm:ss";

        DateTimeZone tz = DateTimeZone.forOffsetHours(
                getFieldAsFloat("DEP_AIRPORT_TZOFFSET")
                        .intValue() / 3600);

        DateTimeFormatter fmt = DateTimeFormat
                .forPattern(format)
                .withZone(DateTimeZone.UTC);

        DateTime utc = DateTime
                .parse(fields[INPUTCOLS.DEP_TIME.ordinal()], fmt);

        DateTime dst = utc.withZone(tz);

        return String.format("%02d", dst.getHourOfDay());
    }

    /**
     * Retrieves the value of a specified field as a float.
     *
     * @param fieldName The name of the field to retrieve.
     * @return The value of the field as a float.
     * @throws EnumConstantNotPresentException if the field name is not found in the INPUTCOLS enum.
     * @throws NumberFormatException if the value of the field cannot be parsed as a float.
     */
    public Float getFieldAsFloat(String fieldName)
            throws IllegalArgumentException, NullPointerException{
        return Float.parseFloat(fields[INPUTCOLS.valueOf(fieldName).ordinal()]);
    }

    /**
     * Creates a new copy of the Flight object.
     *
     * @return a new Flight object copy
     */
    public Flight newCopy() {
        Flight copy = new Flight();
        copy.fields = this.fields.clone();
        copy.avgDepartureDelay = this.avgDepartureDelay;
        copy.avgArrivalDelay = this.avgArrivalDelay;
        return copy;
    }

    /**
     * Sets the average departure delay of a flight.
     *
     * @param avgDepartureDelay The average departure delay as a float.
     */
    public void setAvgDepartureDelay(float avgDepartureDelay) {
        this.avgDepartureDelay = avgDepartureDelay;
    }

    /**
     * Sets the average arrival delay of a flight.
     *
     * @param avgArrivalDelay The average arrival delay as a float.
     */
    public void setAvgArrivalDelay(float avgArrivalDelay) {
        this.avgArrivalDelay = avgArrivalDelay;
    }

    /**
     * Returns NOTIFY_TIME as joda Instance object
     *
     * @return Instance object of Notify Time
     */
    public Instant getEventTimestamp() {
        String timestamp = fields[INPUTCOLS.NOTIFY_TIME.ordinal()];
        DateTimeFormatter formatter = DateTimeFormat
                .forPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(DateTimeZone.UTC);
        DateTime dateTime = DateTime.parse(timestamp, formatter);
        return dateTime.toInstant();
    }
}
