package org.example;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.transforms.GroupAndCombine;
import org.example.transforms.ParsingIntoObjects;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

public class TestGroupAndCombine {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private String departedEvent;

    @Before
    public void Fixtures() {
        this.departedEvent = "2018-01-02,AA,19805,AA,1723,11057,1105703,31057,CLT,14307,1430705,30721,PVD,2018-01-02 05:05:00,2018-01-02 05:04:00,-1.00,,,,,2018-01-02 06:56:00,,,0.00,,,683.00,35.21361111,-80.94916667,-18000.0,41.72222222,-71.42777778,-18000.0,departed,2018-01-02 05:04:00";
    }

    @Test
    public void TestGoodFlights() {
        String expected = "2018-01-02 05:04:00";
        Flight f = Flight.fromCsv(this.departedEvent);
        List<Flight>  flights = Collections.singletonList(f);

        PCollection<String> output = tp.apply(Create.of(flights))
                .apply(ParDo.of(new GroupAndCombine.GoodDepartedFlightsFn()))
                .apply(
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via((Flight flight) -> flight.getField("NOTIFY_TIME")));

        PAssert.that(output)
                .containsInAnyOrder(
                        expected
                );
        tp.run();
    }
    @Test
    public void TestAirportHour() {
        Flight f = Flight.fromCsv(this.departedEvent);
        List<Flight>  flights = Collections.singletonList(f);

        PCollection<KV<String, Double>> output = tp.apply(Create.of(flights))
                .apply(ParDo.of(new GroupAndCombine.AirportHourFn()));

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("CLT:00", -1.0)
                );
        tp.run();
    }
}
