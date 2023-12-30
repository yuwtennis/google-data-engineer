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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

public class TestGroupAndCombine {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private String event;

    @Before
    public void Fixtures() {
        this.event = "2018-01-02,AA,19805,AA,102,12173,1217305,32134,HNL,11298,1129806,30194,MIA,2018-01-03 07:00:00,2018-01-03 08:03:00,63.00,24.00,2018-01-03 08:27:00,2018-01-03 15:00:00,4.00,2018-01-03 14:22:00,2018-01-03 15:04:00,42.00,0.00,,0.00,3784.00,21.31777778,-157.92027778,-36000.0,32.89722222,-97.03777778,-21600.0,departed,2018-01-03 15:04:00";
    }

    @Test
    public void TestGoodFlights() {
        String expected = "2018-01-03 15:04:00";
        Flight f = Flight.fromCsv(this.event);
        List<Flight>  flights = Collections.singletonList(f);

        PCollection<String> output = tp.apply(Create.of(flights))
                .apply(ParDo.of(new GroupAndCombine.GoodFlightsFn()))
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
        Flight f = Flight.fromCsv(this.event);
        List<Flight>  flights = Collections.singletonList(f);

        PCollection<KV<String, Double>> output = tp.apply(Create.of(flights))
                .apply(ParDo.of(new GroupAndCombine.AirportHourFn()));

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("HNL:22", 63.0)
                );
        tp.run();
    }
}
