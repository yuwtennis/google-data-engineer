package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import java.lang.RuntimeException;
import org.joda.time.Duration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Arrays;
import java.util.Date;

/*
 * This class will read events from PubSub and insert into BigQuery
 */

public class SamplePubsubIO
{
    /* 
     * Par Do Function
     * Adds processing timestamp to input string.
     * Incoming: String
     * Output: String, String
    */
    static class TimeStampFn extends SimpleFunction<String, List<String>> {
        @Override
        public List<String> apply(String msg) {
            // Generate Timestamp
            DateTime dt = new DateTime();
            DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            String tm = fmt.print(dt);

            return Arrays.asList(msg, tm);
        }
    }

    // Main method
    public static void main ( String[] args ) {
        // For try-catch clause
        CommandLine cmd = null;

        try {
            cmd = prepParser(args);
        } catch (RuntimeException e) {
            System.err.println( "ERROR: " + e.getMessage() );
            System.exit(1);
        }

        runPipeline(
            cmd.getOptionValue("subscription"), 
            cmd.getOptionValue("out-file"));
    }

    private static void runPipeline(String subscription, String outfile) {
        // Prepare pipeline
        System.out.println("Pipeline start!");
        System.out.printf("File out: %s , Subscription: %s\n",
                            subscription, outfile);

        /*
         * BigQuery parameters
        */
        String tableSpec = "samples.sample_table";

        Pipeline p = prepPipeline();

        // Read lines from Pubsub
        PCollection col =  p.apply(PubsubIO.readStrings().fromSubscription(subscription))
             /*
             * https://beam.apache.org/releases/javadoc/2.12.0/
             * On the other hand, if we wanted to get early results every minute
             * of processing time (for which there were new elements in the given
             * window) we could do the following:
             */
             .apply(Window.<String>into(FixedWindows.of(
                        Duration.standardMinutes(1)))
                            .triggering(
                                AfterWatermark.pastEndOfWindow()
                                    .withEarlyFirings(AfterProcessingTime
                                    .pastFirstElementInPane()
                                    .plusDelayOf(Duration.standardMinutes(1))))
                            .discardingFiredPanes()
                            .withAllowedLateness(Duration.ZERO))
             .apply(MapElements.via(new TimeStampFn()));

        // Write data to a file
        col.apply(TextIO.write()
                   .to(outfile)
                   .withWindowedWrites()
                   .withNumShards(1));

        /*
        col.apply(
                MapElemnents.into(TypeDescriptor.of(TableRow.class))
           )
           .apply(BigQueryIO.writeTableRows()
                            .withMethod(STREAMING_INSERTS)
                            .to(tableSpec)
                            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                 );
        */

        System.out.println("Running pipline !");

        // Run pipeline
        p.run().waitUntilFinish();

        System.out.println("Ending Pipeline...!");
    }

    // Prepare pipeline class for transform
    private static Pipeline prepPipeline() {
        // Pipeline create using default runner (DirectRunnter)
        // Interface: PipelineOptions
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        return p;
    }

    // Command line parser.
    // I used general parser to parse command line parameters.
    private static Options prepOptions() {
        Options options = new Options();

        // Option subscription
        Option subscription= new Option("s", "subscription", true, "Name of Pubsub subscription");
        subscription.setRequired(true);
        options.addOption(subscription);

        // Option out-file
        Option outfile = new Option("o", "out-file", true, "Name of output file");
        outfile.setRequired(true);
        options.addOption(outfile);
        
        return options;
    }

    private static CommandLine prepParser(String[] args) {
        CommandLineParser parser = new DefaultParser(); 
        HelpFormatter formatter = new HelpFormatter();
        Options options = prepOptions();

        // For try-catch clause
        CommandLine cmd = null;

        try {
            cmd = parser.parse( options, args );
        } catch ( ParseException e ){
            formatter.printHelp("utility-name", options);

            // Throw new exception to the root level try-catch clause.
            throw new RuntimeException(e.getMessage());
        }

        return cmd;
    }
}
