package org.example.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.App;
import org.example.flight.Flight;
import org.example.flight.FlightPred;
import org.example.flight.FlightRepository;
import org.example.flight.models.BigQuery;
import org.example.flight.models.BigTable;
import org.example.flight.specifications.SuccessfulFlightSpec;

import org.example.prediction.PredictionHelper;
import org.example.prediction.PredictionService;


public class BatchInputOutput implements InputOutput{
    public BatchInputOutput() {}

    @Override
    public PCollection<Flight> readFlights(Pipeline p, App.MyOptions options) {
        FlightRepository fr = new FlightRepository(p);
        SuccessfulFlightSpec<FlightRepository> fs = new SuccessfulFlightSpec<>("2018-01-04");
        return fs.satisfyingFlightsFrom(fr);
    }

    @Override
    public void writeFlights(PCollection<Flight> outFlights, App.MyOptions options) {
        PCollection<FlightPred> fp = outFlights
                .apply("Predict",new PredictionService.Predict(
                        options.getGoogleProjectId(),
                        options.getAiPlatformLocation(),
                        options.getAiPlatformEndpointId()));

        fp.apply("ToStr", ParDo.of(new PredictionHelper.FlightPredToCsv()))
                .apply("ToGoogleStorage",
                    TextIO.write()
                            .to(options.getOutput()+"flightPreds")
                            .withoutSharding()
                            .withSuffix(".csv")
                );

        fp.apply("ToTableRow", ParDo.of(new BigQuery.FlightPredToTableRowFn()))
                .apply("ToBQ",
                        BigQueryIO
                                .writeTableRows()
                                .to(options.getGoogleProjectId()+":"+"flights.flightPreds")
                                .withSchema(BigQuery.createBqSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        // FIXME Should be used when doing streaming
        fp.apply("ToMutation", ParDo.of(new BigTable.FlightPredToMutationFn()))
                .apply("ToBigTable",
                        BigtableIO
                                .write()
                                .withProjectId(options.getGoogleProjectId())
                                .withInstanceId(options.getBigtableInstanceId())
                                .withTableId(options.getBigtableTableId()));
  }
}
