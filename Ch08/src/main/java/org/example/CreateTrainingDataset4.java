package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.transforms.ParDoWithSideInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CreateTrainingDataset4 {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset4.class);

    public static interface MyOptions extends PipelineOptions {
        @Description("Path of the training.csv")
        String getTraindayCsvPath();
        void setTraindayCsvPath(String s);
    }

    public static void main(String[] args) {
        List<String> fixedLines = Arrays.asList(
                "2018-01-02,AA,19805,AA,1723,11057,1105703,31057,CLT,14307,1430705,30721,PVD,2018-01-02 05:05:00,2018-01-02 05:04:00,-1.00,,,,,2018-01-02 06:56:00,,,0.00,,,683.00,35.21361111,-80.94916667,-18000.0,41.72222222,-71.42777778,-18000.0,departed,2018-01-02 05:04:00",
                "2018-01-02,AA,19805,AA,1723,11057,1105703,31057,CLT,14307,1430705,30721,PVD,2018-01-02 05:05:00,2018-01-02 05:04:00,-1.00,,,,,2018-01-02 06:56:00,,,0.00,,,683.00,35.21361111,-80.94916667,-18000.0,41.72222222,-71.42777778,-18000.0,departed,2018-01-02 05:04:00",
                "2018-02-01,AA,19805,AA,580,11057,1105703,31057,CLT,13198,1319801,33198,MCI,2018-01-02 05:15:00,2018-01-02 05:09:00,-6.00,,,,,2018-01-02 07:45:00,,,0.00,,,808.00,35.21361111,-80.94916667,-18000.0,39.29750000,-94.71388889,-21600.0,departed,2018-01-02 05:09:00"
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
                        LOG.info(f.getField("FL_DATE"));
                    }
                }));

        p.run();
    }
}
