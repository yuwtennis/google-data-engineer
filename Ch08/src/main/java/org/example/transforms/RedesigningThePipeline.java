package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.example.entities.Flight;

public class RedesigningThePipeline {
    /***
     * coGrp combines DepDelay collection and ArrDelay collection by DEST
     * and flatten flights across windows
     * @param depDelays
     * @param arrDelays
     * @return
     */
    public static PCollection<Flight> coGrp(
            PCollection<KV<String, Flight>> depDelays,
            PCollection<KV<String, Double>> arrDelays) {

        final TupleTag<Flight> depTag = new TupleTag<>();
        final TupleTag<Double> arrTag = new TupleTag<>();

        return KeyedPCollectionTuple
                .of(depTag, depDelays)
                .and(arrTag, arrDelays)
                .apply(CoGroupByKey.create())
                .apply("Flatten Windows",ParDo.of(new DoFn<KV<String, CoGbkResult>, Flight>() {
                    @ProcessElement
                    public void flatten(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();
                        Iterable<Flight> flights = e.getValue().getAll(depTag);
                        double avgArrDelay = e.getValue().getOnly(arrTag, Double.valueOf(0));

                        // Iterate over all flights in a window
                        for(Flight f : flights) {
                            Flight cloned = f.newCopy();
                            cloned.setAvgArrivalDelay((float)avgArrDelay);
                            c.output(cloned);
                        }
                    }
                }));

    }
}
