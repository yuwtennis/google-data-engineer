package flights.windowing;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Flight;
import org.example.flight.FlightHelper;
import org.example.flight.FlightRepository;
import org.example.flight.exceptions.EventMalformedException;
import org.example.flight.specifications.SlidingWindowSpec;
import org.example.flight.windowing.SlidingWindow;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestSlidingWindow {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private List<String> events;

    @Before
    public void Fixtures() {
        this.events = Collections.singletonList(
                "2018-01-02,AA,19805,AA,1922,11057,1105703,31057,CLT,14492,1449202,34492,RDU,2018-01-02 05:30:00,2018-01-02 05:21:00,-9.00,18.00,2018-01-02 05:39:00,2018-01-02 06:07:00,5.00,2018-01-02 06:19:00,2018-01-02 06:12:00,-7.00,0.00,,0.00,130.00,35.21361111,-80.94916667,-18000.0,35.87777778,-78.78750000,-18000.0,arrived,2018-01-02 06:12:00"
        );
    }

    @Test
    public void TestSimpleWindow() throws EventMalformedException {
        PCollection<Flight> flights = tp
                .apply("create", Create.of(events))
                .apply("addTimestamp", ParDo.of(new FlightHelper.ToFlightFn()))
                .apply("", new SlidingWindow.IntoWindow());

        tp.run();
    }

    @Test
    public void TestRawWindow() throws EventMalformedException {
        PCollection<Flight> flights = tp
                .apply("create", Create.of(events))
                .apply("addTimestamp", ParDo.of(new FlightHelper.ToFlightFn()))
                .apply(Window.into(
                        SlidingWindows.of(
                                Duration.standardHours(1)
                        ).every(Duration.standardMinutes(5))
                ))
                .apply("IsLatestWindow", ParDo.of(new SlidingWindowSpec.IsSatisfiedByFn()));;

        tp.run();
    }
}
