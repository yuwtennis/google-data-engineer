package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.entities.Flight;
import org.example.transforms.ParDoWithSideInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.example.entities.Flight.INPUTCOLS.FL_DATE;

public class CreateTrainingDataset4 {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset4.class);

    public static interface MyOptions extends PipelineOptions {
        @Description("Path of the training.csv")
        String getTraindayCsvPath();
        void setTraindayCsvPath(String s);
    }

    public static void main(String[] args) {
        List<String> fixedLines = Arrays.asList(
                "2018-01-02,AA,19805,AA,2305,11298,1129806,30194,DFW,14683,1468305,33214,SAT,2018-01-02 06:01:00,2018-01-02 06:11:00,10.00,,,,,2018-01-02 07:11:00,,,0.00,,,247.00,32.89722222,-97.03777778,-21600.0,29.53388889,-98.46916667,-21600.0,departed,2018-01-02 06:11:00",
                "2018-01-02,AA,19805,AA,2305,11298,1129806,30194,DFW,14683,1468305,33214,SAT,2018-01-02 06:01:00,2018-01-02 06:11:00,10.00,,,,,2018-01-02 07:11:00,,,0.00,,,247.00,32.89722222,-97.03777778,-21600.0,29.53388889,-98.46916667,-21600.0,departed,2018-01-02 06:11:00",
                "2018-01-02,AA,19805,AA,1922,11057,1105703,31057,CLT,14492,1449202,34492,RDU,2018-01-02 05:30:00,2018-01-02 05:21:00,-9.00,18.00,2018-01-02 05:39:00,2018-01-02 06:07:00,5.00,2018-01-02 06:19:00,2018-01-02 06:12:00,-7.00,0.00,,0.00,130.00,35.21361111,-80.94916667,-18000.0,35.87777778,-78.78750000,-18000.0,arrived,2018-01-02 06:12:00"
        );

        CreateTrainingDataset4.MyOptions options = PipelineOptionsFactory
                .fromArgs(args).withValidation().as(CreateTrainingDataset4.MyOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollectionView<Map<String, String>> traindays =
                p.apply("Read trainday.csv",
                                TextIO.read().from(options.getTraindayCsvPath()))
                        .apply("Parse trainday.csv",
                                ParDo.of(new ParDoWithSideInput.ParseTrainingDayCsv()))
                        .apply("toView", View.asMap());

        p.apply("ReadLines", Create.of(fixedLines))
                .apply("ToCsv", MapElements
                        .via(new SimpleFunction<String, Flight>() {
                            public Flight apply(String s) {
                                return Flight.fromCsv(s);
                            }
                        }))
                .apply("TrainOnly",
                        ParDo.of(new ParDoWithSideInput.CombineTrainDay(traindays))
                                .withSideInputs(traindays))
                .apply("Stdout", ParDo.of(new DoFn<Flight, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Flight f = c.element();
                        LOG.info(f.getField(FL_DATE));
                    }
                }));

        p.run();
    }
}
