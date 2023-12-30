package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.Flight;

public class ComputeTimeAvg {

    public static class AirportHourFn extends DoFn<Flight, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            if (f.getField("EVENT").equals("wheelsoff")) {
                String key = f.getField("ORIGIN") + ":" + f.getDepartureHour();
                Double value = (double) (f.getFieldAsFloat("DEP_DELAY") +
                        f.getFieldAsFloat("TAXI_OUT"));
                c.output(KV.of(key, value));
            }
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
            PCollection<Flight> flights =
                    pCol.apply("ParseFlights", ParDo.of(new DoFn<String, Flight>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            String line = c.element();
                            Flight f = Flight.fromCsv(line);
                            if (f != null) {
                                c.output(f);
                            }
                        }
                    })).apply("GoodFlights", ParDo.of(new DoFn<Flight, Flight>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            Flight f = c.element();
                            if (f.isNotCancelled() && f.isNotDiverted()) {
                                c.output(f);
                            }
                        }
                    }));

            PCollection<KV<String, Double>> delays = flights
                    .apply("airport:hour", ParDo.of(new AirportHourFn()));

            return delays.apply("DelayToCsv", ParDo.of(
                    new DoFn<KV<String, Double>, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            KV<String, Double> kv = c.element();
                            c.output(kv.getKey() + "," + kv.getValue());
                        }
            }));
        }
    }
}
