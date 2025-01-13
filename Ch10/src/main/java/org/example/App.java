package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.flight.Flight;
import org.example.flight.delays.AvgArrivalDelayComputer;
import org.example.flight.delays.AvgDepartureDelayReferer;
import org.example.flight.delays.DelayAggregator;
import org.example.flight.windowing.SlidingWindow;
import org.example.io.BatchInputOutput;
import org.example.io.InputOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.example.flight.delays.AvgDepartureDelayReferer.lookupOriginDepDelay;

public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public interface MyOptions extends PipelineOptions {

        @Description("Path of the temp location used by BigQuery query cache")
        String getTempLocation();
        void setTempLocation(String s);

        @Description("Path of the output directory")
        @Default.String("/tmp/output/")
        String getOutput();
        void setOutput(String s);

        @Description("Google ProjectId used to access external service to apache beam")
        String getGoogleProjectId();
        void setGoogleProjectId(String s);

        @Description("Departure delay dataset")
        String getDepartureDelayCsvPath();
        void setDepartureDelayCsvPath(String s);

        @Description("AI Platform Location")
        String getAiPlatformLocation();
        void setAiPlatformLocation(String s);

        @Description("AI Platform EndpointId")
        String getAiPlatformEndpointId();
        void setAiPlatformEndpointId(String s);

        @Description("BigTable instance ID")
        String getBigtableInstanceId();
        void setBigtableInstanceId(String s);

        @Description("Bigtable table ID")
        String getBigtableTableId();
        void setBigtableTableId(String s);
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        Pipeline p = Pipeline.create(options);
        InputOutput io = new BatchInputOutput();

        // Read flights
        PCollection<Flight> flights = io.readFlights(p, options);

        // Read average departure delay
        PCollectionView<Map<String, Double>> depDelays = new AvgDepartureDelayReferer(p)
                .asView(options.getDepartureDelayCsvPath());

        // Apply time window
        PCollection<Flight> lastHrFlights = flights.apply(new SlidingWindow.IntoWindow());

        // Calculate average arrival delay
        PCollection<KV<String, Double>> arrDelays =
                lastHrFlights.apply("CalculateArrAvg", new AvgArrivalDelayComputer.Compute());

        // Add delay information
        PCollection<KV<String, Flight>> flightDepDelaySet = lookupOriginDepDelay(
                lastHrFlights, depDelays);

        PCollection<Flight> hourlyFlights = DelayAggregator.coGrp(flightDepDelaySet, arrDelays);

        // Write out flights
        io.writeFlights(hourlyFlights, options);

        PipelineResult result = p.run();
    }
}


