package org.example.prediction;

import org.example.flight.Flight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.example.flight.Flight.INPUTCOLS.*;

/***
 * Represents the element for the request
 */
public class Instance {
    private static final Logger LOG = LoggerFactory.getLogger(Instance.class);

    private final double dep_delay;
    private final double taxiout;
    private final double distance;
    private final double avg_dep_delay;
    private final double avg_arr_delay;
    private final double dep_lat;
    private final double dep_lon;
    private final double arr_lat;
    private final double arr_lon;

    private final String carrier;
    private final String origin;
    private final String dest;

    public Instance(Flight f) {
        this.dep_delay = f.getFieldAsFloat(DEP_DELAY);
        // FIXME TAXI_OUT is null . Here assume that there was no delay with taxi out time
        // this.taxiout = f.getFieldAsFloat(TAXI_OUT);
        this.taxiout = 0;
        this.distance = f.getFieldAsFloat(DISTANCE);
        this.avg_dep_delay = f.getAvgDepartureDelay();
        this.avg_arr_delay = f.getAvgArrivalDelay();
        this.dep_lat = f.getFieldAsFloat(DEP_AIRPORT_LAT);
        this.dep_lon = f.getFieldAsFloat(DEP_AIRPORT_LON);
        this.arr_lat = f.getFieldAsFloat(ARR_AIRPORT_LAT);
        this.arr_lon = f.getFieldAsFloat(ARR_AIRPORT_LON);
        this.carrier = f.getField(OP_UNIQUE_CARRIER);
        this.origin = f.getField(ORIGIN);
        this.dest = f.getField(DEST);
    }


    public static Instance of(Flight f) {
        return new Instance(f);
    }
}
