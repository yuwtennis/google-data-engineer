package org.example.prediction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.example.flight.FlightPred;
import org.example.flight.models.BigQuery;

public class PredictionHelper {
    public static class FlightPredToCsv extends DoFn<FlightPred, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().toCsv());
        }
    }

}
