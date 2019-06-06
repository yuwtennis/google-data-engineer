package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class SampleTextIO
{
    public static void main ( String[] args ) {
        System.out.println( "Main class for DirectRunner" );

        // Pipeline create using default runner (DirectRunnter)
        // Interface: PipelineOptions
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        // Read lines from file
        p.apply(TextIO.read().from("/tmp/test"))
         .apply(TextIO.write().to("/tmp/test-out"));

        p.run().waitUntilFinish();
    }
}
