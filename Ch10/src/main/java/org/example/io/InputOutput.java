package org.example.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.example.App;
import org.example.flight.Flight;


/***
 * An interface used to swap the ingest method , i.e. batch or streaming
 */
public interface InputOutput {
    public PCollection<Flight> readFlights(Pipeline p, App.MyOptions options);
    public void writeFlights(PCollection<Flight> outFlights, App.MyOptions options);
}
