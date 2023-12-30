package org.example;

import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.example.transforms.FilterAirport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestFilterAirport {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private List<String> events;
    @Before
    public void fixtures() {
        this.events = Collections.singletonList(
                "2018-01-02,AA,19805,AA,102,12173,1217305,32134,HNL,11298,1129806,30194,MIA,2018-01-03 07:00:00,2018-01-03 08:03:00,63.00,24.00,2018-01-03 08:27:00,2018-01-03 15:00:00,4.00,2018-01-03 14:22:00,2018-01-03 15:04:00,42.00,0.00,,0.00,3784.00,21.31777778,-157.92027778,-36000.0,32.89722222,-97.03777778,-21600.0,arrived,2018-01-03 15:04:00"
        );
    }
    @Test
    public void AssertTrue() {
        assertTrue(true);
    }

    @Test
    public void TestStringFiltering() {
        PAssert.that(
            tp.apply(Create.of(this.events))
                    .apply(ParDo.of(new FilterAirport.StringFilteringFn("MIA"))))
                    .containsInAnyOrder(
                            events.get(0)
                    );

        tp.run();
    }
    @Test
    public void TestPipeline() {
        tp.apply(Create.of(events)).apply(
                new FilterAirport.FilterAirportTransform("MIA"));
        tp.run();
    }
}