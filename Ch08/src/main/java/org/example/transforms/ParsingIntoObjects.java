package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.Flight;

public class ParsingIntoObjects {

    public static class ParseFlightsFn extends DoFn<String, Flight> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();
            Flight f = Flight.fromCsv(line);
            if (f != null) {
                c.output(f);
            }
        }
    }

    public static class GoodFlightsFn extends DoFn<Flight, Flight> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Flight f = c.element();
            if (f.getField("EVENT").equals("departed")
                    && f.isNotCancelled()) {
                c.output(f);
            } else if (f.getField("EVENT").equals("arrived")
                    && f.isNotDiverted() && f.isNotDiverted()  ) {
                c.output(f);
            }
        }
    }

    public static class ToCsvFn extends DoFn<Flight, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Flight f = c.element();
            if (f.isArrivedEvent()) {
                c.output(f.toTrainingCsv());
            }
        }
    }

    public static class ParsingIntoObjectsTransform
            extends PTransform<PCollection<String>,PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> pCol) {
            return pCol
                    .apply(ParDo.of(new ParseFlightsFn()))
                    .apply(ParDo.of(new GoodFlightsFn()))
                    .apply(ParDo.of(new ToCsvFn()));
        }
    }
}
