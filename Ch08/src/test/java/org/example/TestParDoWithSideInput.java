package org.example;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.*;
import org.example.transforms.ParDoWithSideInput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;


public class TestParDoWithSideInput {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private List<String> trainingCsv;
    private String trainEvent;
    private String testEvent;

    @Before
    public void Fixtures() {
        this.trainingCsv = Collections.singletonList("2018-01-02,True");
        this.trainEvent = "2018-01-02,AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,-9.00,0.00,,0.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-02 03:32:00";
        this.testEvent = "2018-01-03,AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,-9.00,0.00,,0.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-03 03:32:00";
    }
    @Test
    public void TestParseTrainingDayCsv() {
        PCollection<KV<String, String>> output = tp
                .apply(Create.of(this.trainingCsv))
                .apply(ParDo.of(new ParDoWithSideInput.ParseTrainingDayCsv()));

        PAssert.that(output).containsInAnyOrder(KV.of("2018-01-02", ""));
        tp.run();
    }

    @Test
    public void FilterTrainingDataset() {
        PCollectionView<Map<String, String>> trainView = tp
                .apply("CreateTraindaysList", Create.of(this.trainingCsv))
                .apply("asList", ParDo.of(new ParDoWithSideInput.ParseTrainingDayCsv()))
                .apply("TrainView", View.asMap());

        PCollection<String> output =
                tp
                        .apply("CreateEvent", Create.of(this.trainEvent))
                        .apply("ToFlight", MapElements
                                .into(TypeDescriptor.of(Flight.class))
                                .via(Flight::fromCsv))
                        .apply("FilterAsTraining",
                                ParDo.of(
                                        new ParDoWithSideInput.FilterDataset(
                                                trainView,
                                                ParDoWithSideInput.DatasetContext.TRAINING)
                                ).withSideInputs(trainView))
                        .apply("ToNotifyDate", MapElements
                                .into(TypeDescriptors.strings())
                                .via((Flight f)->f.getField("NOTIFY_TIME")));
        PAssert.that(output).containsInAnyOrder("2018-01-02 03:32:00");
        tp.run();
    }

    @Test
    public void FilterTestDataset() {
        PCollectionView<Map<String, String>> trainView = tp
                .apply("CreateTraindaysList", Create.of(this.trainingCsv))
                .apply("asList", ParDo.of(new ParDoWithSideInput.ParseTrainingDayCsv()))
                .apply("TrainView", View.asMap());

        PCollection<String> output =
                tp
                        .apply("CreateEvent", Create.of(this.testEvent))
                        .apply("ToFlight", MapElements
                                .into(TypeDescriptor.of(Flight.class))
                                .via(Flight::fromCsv))
                        .apply("FilterAsTraining",
                                ParDo.of(
                                        new ParDoWithSideInput.FilterDataset(
                                                trainView,
                                                ParDoWithSideInput.DatasetContext.TEST)
                                ).withSideInputs(trainView))
                        .apply("ToNotifyDate", MapElements
                                .into(TypeDescriptors.strings())
                                .via((Flight f)->f.getField("NOTIFY_TIME")));
        PAssert.that(output).containsInAnyOrder("2018-01-03 03:32:00");
        tp.run();
    }

    @Test(expected = Exception.class)
    public void FilterTestDatasetNoTrainViewException() {
        tp
                .apply("CreateEvent", Create.of(this.testEvent))
                .apply("ToFlight", MapElements
                        .into(TypeDescriptor.of(Flight.class))
                        .via(Flight::fromCsv))
                .apply("FilterAsTraining",
                        ParDo.of(
                                new ParDoWithSideInput.FilterDataset(
                                        null,
                                        ParDoWithSideInput.DatasetContext.TEST)
                        ));

        tp.run();
    }
}
