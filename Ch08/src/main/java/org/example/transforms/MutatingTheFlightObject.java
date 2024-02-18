package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.Flight;

import java.util.Map;

public class MutatingTheFlightObject {


    public static class AddDelayInfoFn extends DoFn<Flight, Flight> {
        private final PCollectionView<Map<String, Double>> avgDelay;
        private final String delayType;

        public AddDelayInfoFn(String delayType, PCollectionView<Map<String, Double>> avgDelay) {
            this.delayType = delayType;
            this.avgDelay = avgDelay;
        }
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            if (this.delayType.equals("DEPARTURE")) {
                Flight f = c.element().newCopy();
                String key = f.getField("ORIGIN") + ":" + f.getDepartureHour();
                Double avgDepDelay = c.sideInput(this.avgDelay).get(key);
                f.setAvgDepartureDelay((avgDepDelay == null) ? 0 : avgDepDelay.floatValue());
                c.output(f);
            } else if(this.delayType.equals("ARRIVAL")) {
                Flight f = c.element().newCopy();
                String key = f.getField("DEST");
                Double avgArrDelay = c.sideInput(this.avgDelay).get(key);
                f.setAvgArrivalDelay((avgArrDelay == null) ? 0 : avgArrDelay.floatValue());
                c.output(f);
            }
        }
    }
}
