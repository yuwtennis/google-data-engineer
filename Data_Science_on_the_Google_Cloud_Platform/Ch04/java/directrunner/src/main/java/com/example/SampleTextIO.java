package com.example;

import java.util.List;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleTextIO
{
    @DefaultCoder(AvroCoder.class)
    public static class Car {
        final String tire;
        final String engine;
        final String name;
        final String msg;

        // Default Constructor
        public Car() {
            this.tire   = "";
            this.engine = "";
            this.name   = "";
            this.msg    = "";
        }

        public Car( String tire, String engine, String name, String msg ) {
            this.tire   = tire;
            this.engine = engine;
            this.name   = name;
            this.msg    = msg;
        }
    }

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
                              MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                                         // How?
                                         .via(
                                             (String word) -> (Arrays.asList(word, "Its weekend!"))
                                         )
                          )
                          .apply(ParDo.of(new PrintResultsFn()));

        // MapElements using SimpleFunction using inline code
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
 
        // MapElements using SimpleFunction but not inline code
        PCollection p3 = p.apply(Create.of(LINES))
                          .apply(MapElements.via(new AddFieldFn()))
                          .apply(ParDo.of(new PrintResultsFn()));

        // MapElements using ParDO
        PCollection p4 = p.apply(Create.of(LINES))
                          .apply(ParDo.of(new SampleFn()))
                          .apply(ParDo.of(new PrintResultsFn()));

        // MapElements using KeyValue
        PCollection p5 = p.apply(Create.of(LINES))
                          .apply(
                              MapElements.via(
                                          new SimpleFunction<String, Car>() {
                                              @Override
                                              public Car apply(String word) {

                                                  return new Car( "Bridgestone", "SuperFast", "Mazda", word );
                                              }
                                          }
                              ))
                          .apply(
                              MapElements.into( TypeDescriptors.strings() )
                                         .via(
                                             (Car kuruma) -> (kuruma.name)
                                         )
                          )
                          .apply(ParDo.of(new DoFn<String, Void>(){
                                     @ProcessElement
                                     public void processElement(@Element String word) { 
                                         final Logger LOG = LoggerFactory.getLogger(SampleTextIO.class);
                                         LOG.info(word);
                                     }
                                 })
                          );

        // MapElements using HashMap
        PCollection p6 = p.apply(Create.of(LINES))
                          .apply(MapElements.via( 
                                             new SimpleFunction<String, HashMap<String, String>>(){
                                                 @Override
                                                 public HashMap<String, String> apply(String word) {
                                                         HashMap<String, String> hash = new HashMap<String, String>();

                                                         hash.put("key1", word);
                                                         hash.put("key2", word);
                                                         hash.put("key3", word);

                                                     return hash;
                                                 }
                                             }
                          ))
                          .apply(ParDo.of(new DoFn<HashMap<String, String>, Void>(){
                                     @ProcessElement
                                     public void processElement(@Element HashMap<String, String> word) { 
                                         final Logger LOG = LoggerFactory.getLogger(SampleTextIO.class);
                                         LOG.info(word.toString());
                                     }
                                 })
                          );
                         

        p.run().waitUntilFinish();
    }
}
