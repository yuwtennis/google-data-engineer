package org.example.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.RealTimePipeline;
import org.example.flight.Flight;
import org.example.flight.FlightRepository;
import org.example.flight.specifications.SuccessfulFlightSpec;

import org.example.prediction.PredictionHelper;
import org.example.prediction.PredictionService;


public class BatchInputOutput implements InputOutput{
    public BatchInputOutput() {}

    @Override
    public PCollection<Flight> readFlights(Pipeline p, RealTimePipeline.MyOptions options) {
        FlightRepository fr = new FlightRepository(p);
        SuccessfulFlightSpec<FlightRepository> fs = new SuccessfulFlightSpec<>("2018-01-04");
        return fs.satisfyingFlightsFrom(fr);
    }

    @Override
    public void writeFlights(PCollection<Flight> outFlights, RealTimePipeline.MyOptions options) {
        outFlights
                .apply("Predict",new PredictionService.Predict(
                        options.getAiPlatformProjectId(),
                        options.getAiPlatformLocation(),
                        options.getAiPlatformEndpointId()))
                .apply("ToStr", ParDo.of(new PredictionHelper.FlightPredToCsv()))
                .apply("ToGoogleStorage",
                    TextIO.write()
                            .to(options.getOutput()+"flightPreds")
                            .withoutSharding()
                            .withSuffix(".csv")
                );
    }
}
