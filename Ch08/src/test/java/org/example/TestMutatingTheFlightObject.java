package org.example;


import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.example.transforms.GroupAndCombine;
import org.example.transforms.MutatingTheFlightObject;
import org.example.transforms.ParsingIntoObjects;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestMutatingTheFlightObject {
    public final transient TestPipeline tp = TestPipeline.create();
    private List<String> events;

    @Before
    public void Fixtures() {
        this.events = Arrays.asList(
                "2018-01-02,AA,19805,AA,1922,11057,1105703,31057,CLT,14492,1449202,34492,RDU,2018-01-02 05:30:00,2018-01-02 05:21:00,-9.00,18.00,2018-01-02 05:39:00,2018-01-02 06:07:00,5.00,2018-01-02 06:19:00,2018-01-02 06:12:00,-7.00,0.00,,0.00,130.00,35.21361111,-80.94916667,-18000.0,35.87777778,-78.78750000,-18000.0,arrived,2018-01-02 06:12:00"
        );
    }

    @Test
    public void TestAddArrDelay() {
        Float expected = -7f;
        // Flight
        PCollection<Flight> flight = tp
                .apply(Create.of(this.events.get(0)))
                .apply(ParDo.of(new ParsingIntoObjects.ParseFlightsFn()));

        // Side Input
        PCollectionView<Map<String, Double>> view =
                flight.apply(
                        MapElements
                                .into(TypeDescriptors.kvs(
                                        TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                .via((Flight f)->KV.of("RDU", -7.0)))
                        .apply(View.asMap());

        // Set arrival delay
        PCollection<Float> output =
            flight
                    .apply(
                        ParDo.of(
                                new MutatingTheFlightObject.AddDelayInfoFn("ARRIVAL", view)
                        )
                )
                    .apply(MapElements
                            .into(TypeDescriptor.of(Float.class))
                            .via(Flight::getAvgArrivalDelay));

        PAssert
                .that(output)
                .containsInAnyOrder(
                        expected
                );
    }
    @Test
    public void TestAddDepDelay() {
        Float expected = 9f;
        // Flight
        PCollection<Flight> flight = tp
                .apply(Create.of(this.events.get(0)))
                .apply(ParDo.of(new ParsingIntoObjects.ParseFlightsFn()));

        // Side Input
        PCollectionView<Map<String, Double>> view =
                flight.apply(ParDo.of(new GroupAndCombine.AirportHourFn()))
                        .apply(Mean.perKey())
                        .apply(View.asMap());

        // Set arrival delay
        PCollection<Float> output =
                flight
                        .apply(
                                ParDo.of(
                                        new MutatingTheFlightObject
                                                .AddDelayInfoFn("DEPARTURE", view)
                                )
                        )
                        .apply(MapElements
                                .into(TypeDescriptor.of(Float.class))
                                .via(Flight::getAvgDepartureDelay));

        PAssert
                .that(output)
                .containsInAnyOrder(
                        expected
                );
    }
}
