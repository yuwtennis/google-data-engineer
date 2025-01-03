package org.example.flight.specifications;

import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Flight;

public interface FlightSpec<T> {
    PCollection<Flight> satisfyingFlightsFrom(T elems);
}
