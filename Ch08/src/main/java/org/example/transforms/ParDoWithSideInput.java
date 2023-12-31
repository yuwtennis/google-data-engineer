package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.Flight;

import java.util.Map;

public class ParDoWithSideInput {

    public static class ParseTrainingDayCsv extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element();
            String[] fields = line.split(",");

            if(fields.length > 1 && "True".equals(fields[1])) {
                c.output(KV.of(fields[0], ""));
            }
        }
    }

    public static class CombineTrainDay extends DoFn<Flight, Flight> {
        private final PCollectionView<Map<String, String>> trainView;

        public CombineTrainDay(PCollectionView<Map<String, String>> trainView) {
            this.trainView = trainView;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            Flight f = c.element();
            String date = f.getField("FL_DATE");
            boolean isTrainDay = c.sideInput(this.trainView).containsKey(date);

            if(isTrainDay) {
                c.output(f);
            }
        }
    }
}
