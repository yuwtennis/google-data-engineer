package org.example.flight.specifications;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Events;
import org.example.flight.Flight;
import org.example.flight.FlightRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuccessfulFlightSpec<T> implements FlightSpec<T> {
    private final String cutoffEventDate;

    private static final Logger LOG = LoggerFactory.getLogger(SuccessfulFlightSpec.class);

    public SuccessfulFlightSpec(String cutoffEventDate) {
        this.cutoffEventDate = cutoffEventDate;
    }

    public static class IsSatisfiedByFn extends DoFn<Flight, Flight> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Flight flight = c.element();

            // There are successful flight but delays are null
            if(
                    (flight.isDepartedEvent()
                            && flight.isNotCancelled()
                            && ! flight.getFields()[Flight.INPUTCOLS.DEP_DELAY.ordinal()].isEmpty())
                            ||
                    (flight.isArrivedEvent())
                            && flight.isNotCancelled()
                            && flight.isNotDiverted()
                            && ! flight.getFields()[Flight.INPUTCOLS.DEP_DELAY.ordinal()].isEmpty()
                            && ! flight.getFields()[Flight.INPUTCOLS.ARR_DELAY.ordinal()].isEmpty()) {
                c.output(flight);
            } else {
                LOG.warn("Flight [{}] does not satisfy the specification. So drop.",
                        String.join(",", flight.getFields()));
            }
        }
    }

    /**
     * Validates the event and return only the expected ones
     *
     * @param elems
     * @return
     */
    public PCollection<Flight> satisfyingFlightsFrom(T elems) {
        PCollection<Flight> allFlights =  ((FlightRepository)elems)
                .selectWhereExpectedEventOccurredBefore(cutoffEventDate);

        // Rules
        //  Good flights consists of
        //    Non Cancelled for departed events
        //    Non Divert for arrival events
        return allFlights
                .apply("IsSuccessfulFlights", ParDo.of(new IsSatisfiedByFn()));

    }
}
