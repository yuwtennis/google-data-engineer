package org.example.flight.windowing;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Flight;
import org.example.flight.specifications.SlidingWindowSpec;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlidingWindow {
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindow.class);

    public static class IntoWindow extends PTransform<PCollection<Flight>, PCollection<Flight>> {
        @Override
        public PCollection<Flight> expand(PCollection<Flight> input) {
            PCollection<Flight> flights = input.apply(Window.into(
                    SlidingWindows.of(
                            Duration.standardHours(SlidingWindowSpec.getWindowSize())
                    ).every(Duration.standardMinutes(SlidingWindowSpec.getWindowEmitInterval()))
            ));

            SlidingWindowSpec<PCollection<Flight>> spec =
                    new SlidingWindowSpec<>();
            return spec.satisfyingFlightsFrom(flights);

        }
    }
}
