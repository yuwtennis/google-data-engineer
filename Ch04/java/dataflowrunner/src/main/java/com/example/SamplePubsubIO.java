package com.example;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;

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
import org.joda.time.Instant;

import java.util.List;
import java.util.Arrays;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class will read events from PubSub and insert into BigQuery
 */

public class SamplePubsubIO
{
    // Inner class
    // Used for "container" for dataset from PubSub
    @DefaultCoder(AvroCoder.class)
    static class PubsubData {
        private final String timestamp;
        private final String msg;

        // Default constructor
        public PubsubData() {
            this.timestamp = "";
            this.msg = "";
        }

        // Constructor
        public PubsubData( String timestamp, String msg ) {
            this.timestamp = timestamp;
            this.msg = msg;
        }

        public String getTimestamp() {
            return this.timestamp;
        }

        public String getMsg() {
            return this.msg;
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
                            outfile, subscription);

        /*
         * BigQuery parameters
         */
        // Prepare fully qualified reference to big query table
        TableReference tableSpec = new TableReference()
                                       .setProjectId("elite-caster-125113")
                                       .setDatasetId("samples") 
                                       .setTableId("sample_table");
        // Prepare Field schema
        TableSchema tableSchema  = new TableSchema()
                                       .setFields(
                                           ImmutableList.of(
                                               new TableFieldSchema().setName("timestamp")
                                                                     .setType("DATETIME")
                                                                     .setMode("REQUIRED"),
                                               new TableFieldSchema().setName("message")
                                                                     .setType("STRING")
                                                                     .setMode("NULLABLE")
                                           )
                                       );

        Pipeline p = prepPipeline();

        // Read lines from Pubsub
        /*
         * https://beam.apache.org/releases/javadoc/2.12.0/
         * On the other hand, if we wanted to get early results every minute
         * of processing time (for which there were new elements in the given
         * window) we could do the following:
        */
        PCollection<PubsubData> col =  p.apply("pubsub:ingest", PubsubIO.readStrings().fromSubscription(subscription))
                            .apply("pubsub:window_processing", Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                                         .triggering(
                                             AfterWatermark.pastEndOfWindow()
                                                           .withEarlyFirings(
                                                               AfterProcessingTime
                                                                   .pastFirstElementInPane()
                                                                   .plusDelayOf(Duration.standardMinutes(1))
                                                           )
                                         )
                                         .discardingFiredPanes()
                                         .withAllowedLateness(Duration.ZERO))
                            .apply( "pubsub:extract", MapElements.via (
                                new SimpleFunction<String, PubsubData>() {
                                    @Override
                                    public PubsubData apply( String line ) {
                                        // Generate Timestamp
                                        DateTime dt = new DateTime();
                                        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                                        String tm = fmt.print(dt);

                                        // Add a timestamp field
                                        return new PubsubData( tm, line );
                                    }
                                }
                            ));

        /* Write out to a file*/
        PCollection<String> out = col.apply("file:toString", MapElements.into( TypeDescriptors.strings() )
                                                       .via( (PubsubData elem) -> (elem.getMsg()))
                                     );
        // TextIO.Write takes PCollection as input. Thus it cannot defined inside the pipeline.
        out.apply("file:toFile", TextIO.write()
                        .to(outfile)
                        .withWindowedWrites()
                        .withNumShards(1)
           );

        /* Write out to BigQuery */
        /*
         * Write method based on apache beam 2.12.0 
         * https://beam.apache.org/releases/javadoc/2.12.0/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html
        */
        col.apply("bq:toBq", BigQueryIO.<PubsubData>write()
                            .to(tableSpec)
                            .withSchema(tableSchema)
                            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                            .withFormatFunction( elem -> new TableRow().set("timestamp", elem.getTimestamp() ).set( "message", elem.getMsg() ) )
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
           );

        System.out.println("Running pipline !");

        // Run pipeline
        p.run().waitUntilFinish();

        System.out.println("Ending Pipeline...!");
    }

    // Prepare pipeline class for transform
    private static Pipeline prepPipeline() {
        // Pipeline created using Dataflowrunner
        // Interface: PipelineOptions
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        options.setProject("elite-caster-125113");
        options.setStagingLocation("gs://elite-caster-125113/dataflow/staging");
        options.setTempLocation("gs://elite-caster-125113/dataflow/template");
        options.setRunner(DataflowRunner.class);

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
