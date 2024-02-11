package org.example;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.transforms.RedesigningThePipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestRedesigningThePipeline {

    public static class SetDepDelay
            extends PTransform<PCollection<Flight>, PCollection<KV<String, Flight>>> {
        @Override
        public PCollection<KV<String, Flight>> expand(PCollection<Flight> flight) {
            return flight.apply("ToDepDelay", ParDo.of(new DoFn<Flight, KV<String, Flight>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Flight f = c.element().newCopy();
                    f.setAvgDepartureDelay(1.0f);
                    String key = f.getField("DEST");
                    c.output(KV.of(key, f));
                }
            }));
        }
    }

    public static class SetAvgDelay
            extends PTransform<PCollection<Flight>, PCollection<KV<String, Double>>> {
        @Override
        public PCollection<KV<String, Double>> expand(PCollection<Flight> flight) {
            return flight.apply("ToDepDelay", ParDo.of(new DoFn<Flight, KV<String, Double>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String key = c.element().getField("DEST");
                    Double value = 1.0;
                    c.output(KV.of(key, value));
                }
            }));
        }
    }

    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private List<String> events;

    @Before
    public void Fixtures() {
        this.events = Collections.singletonList(
                "2018-01-02,AA,19805,AA,1922,11057,1105703,31057,CLT,14492,1449202,34492,RDU,2018-01-02 05:30:00,2018-01-02 05:21:00,-9.00,18.00,2018-01-02 05:39:00,2018-01-02 06:07:00,5.00,2018-01-02 06:19:00,2018-01-02 06:12:00,-7.00,0.00,,0.00,130.00,35.21361111,-80.94916667,-18000.0,35.87777778,-78.78750000,-18000.0,arrived,2018-01-02 06:12:00");
    }
    @Test
    public void TestCoGrp() {
        PCollection<Flight> input =
                tp
                        .apply(Create.of(this.events))
                        .apply("ToFlights",MapElements
                                .into(TypeDescriptor.of(Flight.class))
                                .via(Flight::fromCsv));

        PCollection<KV<String, Flight>> depDelay =
                input
                        .apply("ToDepDelay", new SetDepDelay());


        PCollection<KV<String, Double>> arrDelay =
                input
                        .apply("ToArrDelay", new SetAvgDelay());

        PCollection<Flight> output = RedesigningThePipeline.coGrp(depDelay, arrDelay);

        PCollection<Float> depDelays = output
                .apply("ToFloatDepDelay",
                        MapElements.into(TypeDescriptors.floats())
                                .via(Flight::getAvgDepartureDelay));

        PCollection<Float> arrDelays = output
                .apply("ToFloatArrDelay",
                        MapElements.into(TypeDescriptors.floats())
                                .via(Flight::getAvgArrivalDelay));

        PAssert.that(depDelays).containsInAnyOrder(1.0f);
        PAssert.that(arrDelays).containsInAnyOrder(1.0f);

        tp.run();
    }
}
