package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.Flight;

public class GroupAndCombine {

    public static class GoodDepartedFlightsFn extends DoFn<Flight, Flight> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Flight f = c.element();
            // Field DIVERTED was empty. Only evaluate CANCELLED field.
            if (f.getField("EVENT").equals("departed") && f.isNotCancelled()) {
                c.output(f);
            }
        }
    }

    public static class AirportHourFn extends DoFn<Flight, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            if (f.getField("EVENT").equals("departed")) {
                String key = f.getField("ORIGIN") + ":" + f.getDepartureHour();
                // TAXI_OUT field was emtpy for  "depareted"  events
                Double value = (double) (f.getFieldAsFloat("DEP_DELAY"));
                c.output(KV.of(key, value));
            }
        }
    }

    public static class DelayToCsvFn extends DoFn<KV<String, Double>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, Double> kv = c.element();
            c.output(kv.getKey() + "," + kv.getValue());
        }
    }

    public static class ComputeTimeAvgTransform
            extends PTransform<PCollection<String>, PCollection<String>> {

        /**
         * Composite transform
         * @param pCol
         * @return
         */
        @Override
        public PCollection<String> expand(PCollection<String> pCol) {
            return pCol
                    .apply("ParseFlightsFn", ParDo.of(new ParsingIntoObjects.ParseFlightsFn()))
                    .apply("GoodDepartedFlightsFn", ParDo.of(new GoodDepartedFlightsFn()))
                    .apply("airport:hour", ParDo.of(new AirportHourFn()))
                    .apply(Mean.perKey())
                    .apply("DelayToCsv", ParDo.of(new DelayToCsvFn()));
        }
    }
}
