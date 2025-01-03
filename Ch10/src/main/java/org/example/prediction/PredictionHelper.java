package org.example.prediction;

import org.apache.beam.sdk.transforms.DoFn;
import org.example.flight.FlightPred;

public class PredictionHelper {
    public static class FlightPredToCsv extends DoFn<FlightPred, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toCsv());
        }
    }
}
