package org.example;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Flight {
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
     *
     * @param line
     * @return
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
     *
     * @return
     */
    public boolean isNotDiverted() {
        return fields[INPUTCOLS.DIVERTED.ordinal()].equals("0.00");
    }

    /**
     *
     * @return
     */
    public boolean isNotCancelled() {
        return fields[INPUTCOLS.CANCELLED.ordinal()].equals("0.00");
    }

    /**
     *
     * @return
     */
    public boolean isArrivedEvent() {
        return fields[INPUTCOLS.EVENT.ordinal()].equals("arrived");
    }

    /**
     *
     * @return
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
     *
     * @return
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
     *
     * @return
     */
    public String[] getFields() {
        return fields;
    }

    /**
     *
     * @return
     */
    public float getAvgDepartureDelay() {
        return avgDepartureDelay;
    }

    /**
     *
     * @return
     */
    public float getAvgArrivalDelay() {
        return avgArrivalDelay;
    }

    /**
     *
     * @return
     */
    public int getValidFieldNum() {
        return INPUTCOLS.values().length;
    }
}
