package org.example.flight.specifications;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Flight;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlidingWindowSpec<T> implements FlightSpec<T> {
    private static final int WINDOW_SIZE = 1;
    private static final int WINDOW_EMIT_INTERVAL = 5;

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowSpec.class);

    public static class IsSatisfiedByFn extends DoFn<Flight, Flight> {
        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow window) {
            Flight flight = c.element();

            Instant endOfWindow = window.maxTimestamp();
            Instant flightTimestamp = flight.getEventTimestamp();
            long msec = endOfWindow.getMillis() - flightTimestamp.getMillis();
            long THRESH = (long) WINDOW_SIZE * 60 * 1000; // 5 minutes

            if (msec < THRESH) {
                c.output(c.element());
            }
        }
    }

    public PCollection<Flight> satisfyingFlightsFrom(T elems) {
        return ((PCollection<Flight>) elems).apply("FilterSlidingWindow", ParDo.of(new IsSatisfiedByFn()));
    }

    public static long getWindowSize() { return WINDOW_SIZE;
    }

    public static long getWindowEmitInterval() {
        return WINDOW_EMIT_INTERVAL;
    }
}
