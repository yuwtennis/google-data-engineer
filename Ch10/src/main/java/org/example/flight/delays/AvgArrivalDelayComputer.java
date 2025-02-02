package org.example.flight.delays;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Flight;
import org.example.flight.FlightHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.example.flight.Events.ARRIVED;
import static org.example.flight.Flight.INPUTCOLS.*;

public class AvgArrivalDelayComputer {
    private static final Logger LOG = LoggerFactory.getLogger(AvgArrivalDelayComputer.class);
    public static class ToKV extends DoFn<Flight, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Flight f = c.element();
            if (f.isArrivedEvent()) {
                String key = f.getField(DEST);
                double value = f.getFieldAsFloat(ARR_DELAY);
                c.output(KV.of(key, value));
            }
        }
    }

    public static class Compute
            extends PTransform<PCollection<Flight>, PCollection<KV<String, Double>>> {

        @Override
        public PCollection<KV<String, Double>> expand(PCollection<Flight> input) {
            return input.apply("CalcArrDelay", ParDo.of(new ToKV()))
                    .apply(Mean.perKey());
        }
    }
}
