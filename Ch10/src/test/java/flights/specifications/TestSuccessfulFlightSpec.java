package flights.specifications;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Flight;
import org.example.flight.FlightHelper;
import org.example.flight.FlightRepository;
import org.example.flight.exceptions.EventMalformedException;
import org.example.flight.specifications.SlidingWindowSpec;
import org.example.flight.specifications.SuccessfulFlightSpec;
import org.example.flight.windowing.SlidingWindow;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TestSuccessfulFlightSpec {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private String malformedDepEvent;

    @Before
    public void Fixtures() {
        malformedDepEvent = "2018-01-03,9E,20363,9E,3897,11042,1104205,30647,CLE,13487,1348702,31650,MSP,2018-01-03 14:20:00,2018-01-03 14:20:00,,,,,,2018-01-03 ";
    }

    @Test
    public void TestMalformedDepEvent() {
        PCollection<Flight> flights = tp
                .apply("create", Create.of(malformedDepEvent))
                .apply("ToFlight", ParDo.of(new FlightHelper.ToFlightFn()))
                .apply("TestSpec", ParDo.of(new SuccessfulFlightSpec.IsSatisfiedByFn()));

        PAssert.that(flights).empty();
        tp.run();
    }
}
