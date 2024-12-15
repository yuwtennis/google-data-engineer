package org.example;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.entities.Flight;
import org.example.transforms.GroupAndCombine;
import org.example.transforms.ParsingIntoObjects;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

import static org.example.entities.Flight.INPUTCOLS.NOTIFY_TIME;

public class TestGroupAndCombine {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private String event;

    @Before
    public void Fixtures() {
        this.event = "2018-01-02,AA,19805,AA,1922,11057,1105703,31057,CLT,14492,1449202,34492,RDU,2018-01-02 05:30:00,2018-01-02 05:21:00,-9.00,18.00,2018-01-02 05:39:00,2018-01-02 06:07:00,5.00,2018-01-02 06:19:00,2018-01-02 06:12:00,-7.00,0.00,,0.00,130.00,35.21361111,-80.94916667,-18000.0,35.87777778,-78.78750000,-18000.0,arrived,2018-01-02 06:12:00";
    }

    @Test
    public void TestGoodFlights() {
        String expected = "2018-01-02 06:12:00";
        Flight f = Flight.fromCsv(this.event);
        List<Flight>  flights = Collections.singletonList(f);

        PCollection<String> output = tp.apply(Create.of(flights))
                .apply(ParDo.of(new ParsingIntoObjects.GoodFlightsFn()))
                .apply(
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via((Flight flight) -> flight.getField(NOTIFY_TIME)));

        PAssert.that(output)
                .containsInAnyOrder(
                        expected
                );
        tp.run();
    }
    @Test
    public void TestAirportHour() {
        Flight f = Flight.fromCsv(this.event);
        List<Flight>  flights = Collections.singletonList(f);

        PCollection<KV<String, Double>> output = tp.apply(Create.of(flights))
                .apply(ParDo.of(new GroupAndCombine.AirportHourFn()));


        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("CLT:00", 9.0)
                );
        tp.run();
    }
}
