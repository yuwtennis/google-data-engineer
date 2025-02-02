package org.example.flight;

import org.apache.beam.sdk.transforms.DoFn;
import org.example.flight.exceptions.EventMalformedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightHelper {
    private static final Logger LOG = LoggerFactory.getLogger(FlightHelper.class);

    public static class ToFlightFn extends DoFn<String, Flight> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String s = c.element();
            try {
                Flight f = Flight.fromCsv(s);
                c.outputWithTimestamp(f, f.getEventTimestamp());
            } catch (EventMalformedException e) {
                LOG.warn("Event malformed. Skip...");
                LOG.warn(e.getMessage());
            }
        }
    }
}
