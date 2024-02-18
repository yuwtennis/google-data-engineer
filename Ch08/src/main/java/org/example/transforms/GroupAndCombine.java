package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.Flight;

public class GroupAndCombine {

    public static class AirportHourFn extends DoFn<Flight, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();

            if (f.getField("EVENT").equals("arrived")) {
                String key = f.getField("ORIGIN") + ":" + f.getDepartureHour();
                // TAXI_OUT field was emtpy for  "departed"  events
                Double value = (double)(f.getFieldAsFloat("DEP_DELAY")
                        + f.getFieldAsFloat("TAXI_OUT"));
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
                    .apply("ParseFlights", ParDo.of(new ParsingIntoObjects.ParseFlightsFn()))
                    .apply("GoodFlights", ParDo.of(new ParsingIntoObjects.GoodFlightsFn()))
                    .apply("airport:hour", ParDo.of(new AirportHourFn()))
                    .apply(Mean.perKey())
                    .apply("DelayToCsv", ParDo.of(new DelayToCsvFn()));
        }
    }
}
