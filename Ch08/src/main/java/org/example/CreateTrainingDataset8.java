/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.transforms.GroupAndCombine;
import org.example.transforms.MutatingTheFlightObject;
import org.example.transforms.ParDoWithSideInput;
import org.example.transforms.ParsingIntoObjects;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CreateTrainingDataset8 {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset8.class);
  private static final String DELAY_TYPE_DEPARTURE = "DEPARTURE";
  private static final String DELAY_TYPE_ARRIVAL = "ARRIVAL";
  public static interface MyOptions extends PipelineOptions {

      @Description("Path of the temp location used by BigQuery query cache")
      String getTempLocation();
      void setTempLocation(String s);

      @Description("Path of the training.csv")
      String getTraindayCsvPath();
      void setTraindayCsvPath(String s);

      @Description("Path of the output directory")
      @Default.String("/tmp/output/")
      String getOutput();
      void setOutput(String s);
  }
  public static void main(String[] args) {

      String query = "SELECT EVENT_DATA FROM flights.simevents "
              + "WHERE STRING(FL_DATE) = '2018-01-02' AND (EVENT = 'departed' "
              + "OR EVENT = 'arrived') AND OP_UNIQUE_CARRIER = 'AA'";

      MyOptions options = PipelineOptionsFactory
              .fromArgs(args)
              .withValidation()
              .as(MyOptions.class);

      Pipeline p = Pipeline.create(options);

      PCollection<String> events =
              p.apply("ReadLines",
                      BigQueryIO
                              .read(
                                      (SchemaAndRecord elem)->elem
                                              .getRecord()
                                              .get("EVENT_DATA")
                                              .toString())
                              .fromQuery(query)
                              .usingStandardSql()
                              .withCoder(StringUtf8Coder.of()));
      // Load training days
      PCollectionView<Map<String, String>> traindays =
              p.apply("Read trainday.csv",
                              TextIO.read().from(options.getTraindayCsvPath()))
                      .apply("Parse trainday.csv",
                              ParDo.of(new ParDoWithSideInput.ParseTrainingDayCsv()))
                      .apply("toView", View.asMap());

      PCollection<Flight> flights = events
                      .apply("ToFLights", ParDo.of(new DoFn<String, Flight>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                              String line = c.element();
                              Flight f = Flight.fromCsv(line);
                              if(f != null) {
                                  c.outputWithTimestamp(f, f.getEventTimestamp());
                              }
                          }
                      }))
              .apply("GoodFlights", ParDo.of(new ParsingIntoObjects.GoodFlightsFn()))
              .apply("FilterTrainDays",
                      ParDo.of(
                              new ParDoWithSideInput
                                      .CombineTrainDay(traindays)).withSideInputs(traindays));

      //Create view for average departure delay
      PCollectionView<Map<String, Double>> avgDepDelay =
              flights
                      .apply(ParDo.of(new GroupAndCombine.AirportHourFn()))
                      .apply(Mean.perKey())
                      .apply(View.asMap());

      // Arrival delay average over an hour for every 5 minute
      PCollection<Flight> lastHourFlights =
              flights
                      .apply(Window.into(
                              SlidingWindows.of(
                                      Duration.standardHours(1)
                              ).every(Duration.standardMinutes(5))));

      PCollectionView<Map<String, Double>> avgArrDelay =
              lastHourFlights
                  .apply(ParDo.of(new DoFn<Flight, KV<String, Double>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                          Flight f = c.element();

                          // Exclude events that has empty ARR_DELAY
                          if(f.getField("EVENT").equals("arrived")){
                              String key = f.getField("DEST");
                              double value = f.getFieldAsFloat("ARR_DELAY");
                              LOG.info("Dest {} , Arrival Delay {}", key, value);
                              c.output(KV.of(key, value));
                          }
                      }}))
                  .apply(Mean.perKey())
                  .apply(View.asMap());

      // Filter the lines with training days
      PCollection<String> trainCsv =
              lastHourFlights
                  .apply("AddDepDelay", ParDo.of(
                          new MutatingTheFlightObject.AddDelayInfoFn(DELAY_TYPE_DEPARTURE, avgDepDelay))
                          .withSideInputs(avgDepDelay))
                  .apply("AddArrDelay",ParDo.of(
                          new MutatingTheFlightObject.AddDelayInfoFn(DELAY_TYPE_ARRIVAL, avgArrDelay))
                          .withSideInputs(avgArrDelay))
                  .apply("ToCsv", ParDo.of(new DoFn<Flight, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                          Flight f = c.element();
                          String csv = f.toTrainingCsv();
                          c.output(csv);
                      }
                  }));
      // To train csv
      trainCsv.apply(
              TextIO
                      .write()
                      .to(options.getOutput()+"flights8")
                      .withSuffix(".csv").withoutSharding());

      p.run();
  }
}
