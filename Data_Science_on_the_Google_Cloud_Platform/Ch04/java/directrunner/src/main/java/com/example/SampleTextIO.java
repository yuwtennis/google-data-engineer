package com.example;

import java.util.List;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleTextIO
{

    // just print the results
    static class PrintResultsFn extends DoFn<List<String>, Void> {
        private static final Logger LOG = LoggerFactory.getLogger(SampleTextIO.class);

        @ProcessElement
        public void processElement(@Element List<String> words) {
            LOG.info(Arrays.toString(words.toArray()));
        }
    }

    static class SampleFn extends DoFn<String, List<String>> {
        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<List<String>> out) {
            out.output(Arrays.asList(word, "Its weekend!"));
        }
    }

    static class AddFieldFn extends SimpleFunction<String, List<String>> {
        @Override
        public List<String> apply(String word) {
            return Arrays.asList( word, "Its weekend!");
        }
    }

    public static void main ( String[] args ) {
        System.out.println( "Main class for DirectRunner" );

        // Pipeline create using default runner (DirectRunnter)
        // Interface: PipelineOptions
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        // Example pcollection
        final List<String> LINES = Arrays.asList(
            "blah"
        );

        /*
         * Basics of MapElements.
         * https://beam.apache.org/releases/javadoc/2.13.0/org/apache/beam/sdk/transforms/MapElements.html#into-org.apache.beam.sdk.values.TypeDescriptor-
         * into: Returns a new MapElements transform with 
                 the given type descriptor for the output type,
                 but the mapping function yet to be specified using via(ProcessFunction).
         *
         * via: For a ProcessFunction<InputT, OutputT> fn and output type descriptor,
                returns a PTransform that takes an input PCollection<InputT>
                and returns a PCollection<OutputT> containing fn.apply(v) for every element v in the input.

         */
        // MapElements using ProcessFunction
        PCollection p1 = p.apply(Create.of(LINES))
                          .apply(
                                          // To What?
                              MapElements.into(
                                              TypeDescriptors.lists(TypeDescriptors.strings())
                                          )
                                          // How?
                                          .via(
                                              (String word) -> (Arrays.asList(word, "Its weekend!"))
                                          )
                          )
                          .apply(ParDo.of(new PrintResultsFn()));

        // MapElements using SimpleFunction
        PCollection p2 = p.apply(Create.of(LINES))
                          .apply(
                              MapElements.via(
                                             new SimpleFunction<String, List<String>>() {
                                                 public List<String> apply(String word) {
                                                     return Arrays.asList(word, "Its weekend!");
                                                 }
                                             }
                                         )
                          )
                          .apply(ParDo.of(new PrintResultsFn()));
 
        // MapElements using DoFn
        PCollection p3 = p.apply(Create.of(LINES))
                          .apply(MapElements.via(new AddFieldFn()))
                          .apply(ParDo.of(new PrintResultsFn()));

        // MapElements using ParDO
        PCollection p4 = p.apply(Create.of(LINES))
                          .apply(ParDo.of(new SampleFn()))
                          .apply(ParDo.of(new PrintResultsFn()));

        p.run().waitUntilFinish();
    }
}
