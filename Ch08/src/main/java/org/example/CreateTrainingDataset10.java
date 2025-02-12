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
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.example.transforms.GroupAndCombine;
import org.example.transforms.ParDoWithSideInput;
import org.example.transforms.ParsingIntoObjects;
import org.example.transforms.RedesigningThePipeline;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import static org.example.Flight.INPUTCOLS.*;

public class CreateTrainingDataset10 {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset10.class);
  public interface MyOptions extends PipelineOptions {

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

      @Description("Should we process the full dataset?")
      @Default.Boolean(false)
      boolean getFullDataset();
      void setFullDataset(boolean b);
  }
  public static void main(String[] args) {

      MyOptions options = PipelineOptionsFactory
              .fromArgs(args)
              .withValidation()
              .as(MyOptions.class);

      Pipeline p = Pipeline.create(options);

      String query = "SELECT EVENT_DATA FROM flights.simevents ";
      if(!options.getFullDataset()) {
          query += "WHERE STRING(FL_DATE) <= '2018-01-04' AND ";
      }
      query += "(EVENT = 'departed' OR EVENT = 'arrived')";

      CreateTrainingDataset10.LOG.info(query);

      PCollection<String> allFlights =
              p.apply("LoadBQ",
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
      PCollectionView<Map<String, String>> trainDays =
              p.apply("Read trainday.csv",
                              TextIO.read().from(options.getTraindayCsvPath()))
                      .apply("Parse trainday.csv",
                              ParDo.of(new ParDoWithSideInput.ParseTrainingDayCsv()))
                      .apply("toView", View.asMap());

      PCollection<Flight> goodFlights = allFlights
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
              .apply("GoodFlights", ParDo.of(new ParsingIntoObjects.GoodFlightsFn()));

      PCollection<Flight> trainFlights = goodFlights
              .apply("FilterTrainDays",
                      ParDo.of(
                              new ParDoWithSideInput
                                      .FilterDataset(
                                              trainDays, true
                              )
                      ).withSideInputs(trainDays));

      // Departure average over training dataset
      // Text output for every airport-hour
      // To csv
      PCollection<KV<String, Double>> airportHours =
              trainFlights
                      .apply(ParDo.of(new GroupAndCombine.AirportHourFn()))
                      .apply(Mean.perKey());

      airportHours
              .apply("ToCSV", MapElements.via(
                      new SimpleFunction<KV<String, Double>, String>() {
                          @Override
                          public String apply(KV<String, Double> input) {
                              return input.getKey()+","+input.getValue().toString();
                          }
                      }))
              .apply(
                      TextIO
                              .write()
                              .to(options.getOutput()+"delays")
                              .withSuffix(".csv")
                              .withoutSharding());

      //Create view for average departure delay
      PCollectionView<Map<String, Double>> avgDepDelay =
              airportHours.apply("ToDepartureDelayView", View.asMap());

      // Average arrival delay over entire dataset
      // Delay over an hour for every 5 minute
      PCollection<Flight> lastHourFlights =
              goodFlights
                      .apply(Window.into(
                              SlidingWindows.of(
                                      Duration.standardHours(1)
                              ).every(Duration.standardMinutes(5))))
                      .apply("IsLatestWindow", ParDo.of(new DoFn<Flight, Flight>() {
                          @ProcessElement
                          public void processElement(
                                  ProcessContext c,
                                  IntervalWindow window
                          ) throws Exception {
                              Instant endOfWindow = window.maxTimestamp();
                              Instant flightTimestamp = c.element().getEventTimestamp();
                              long msec = endOfWindow.getMillis() - flightTimestamp.getMillis();
                              long THRESH = 5 * 60 * 1000; // 5 minutes
                              if (msec < THRESH) {
                                  c.output(c.element());
                              }
                          }
                      }));

      PCollection<KV<String, Double>> arrDelays =
              lastHourFlights
                  .apply("ToArrKV", ParDo.of(new DoFn<Flight, KV<String, Double>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                          Flight f = c.element();

                          // Exclude events that has empty ARR_DELAY
                          if (f.getField(EVENT).equals("arrived")) {
                              String key = f.getField(DEST);
                              double value = f.getFieldAsFloat(ARR_DELAY);
                              c.output(KV.of(key, value));
                          }
                   }}))
                      .apply("MeanPerKey", Mean.perKey());

      // Set departure delay
      PCollection<KV<String, Flight>> depDelays =
              lastHourFlights
				  .apply("SetDepDelayPerFlight", ParDo.of(new DoFn<Flight, Flight>() {
					  @ProcessElement
					  public void processElement(ProcessContext c) throws Exception {
                          Flight f = c.element().newCopy();
                          String origin = f.getField(ORIGIN);
                          Double depDelay =  c.sideInput(avgDepDelay).get(origin+":"+f.getDepartureHour());
                          f.setAvgDepartureDelay((float) (depDelay == null ? 0 : depDelay));
                          c.output(f);
                      }
				   }).withSideInputs(avgDepDelay))
				  .apply("ToDepKV", ParDo.of(new DoFn<Flight, KV<String, Flight>>() {
					  @ProcessElement
					  public void processElement(ProcessContext c) {
						  Flight f = c.element();
						  String dest = f.getField(DEST);
						  c.output(KV.of(dest, f));
					  }
				  }));

      // Combine 2 collections by DEST
      PCollection<Flight> groupedFlights = RedesigningThePipeline.coGrp(depDelays, arrDelays);

      for(String t : new String[]{"train", "test"}) {
          PCollection<String> csv = flightToCsv(
                  groupedFlights,
                  trainDays,
                  t.equals("train")
                  );

          csv.apply(t+"Csv",
                  TextIO
                          .write()
                          .to(options.getOutput()+t)
                          .withSuffix(".csv")
          );
      }

      PipelineResult result = p.run();
      if(!options.getFullDataset()) {
          // Wait for small datasets
          result.waitUntilFinish();
      }
  }

  private static PCollection<String> flightToCsv(
          PCollection<Flight> flights,
          PCollectionView<Map<String, String>> traindays,
          boolean trainSetExtracted) {

          return flights
              .apply("FilterDataSet",
                      ParDo.of(
                              new ParDoWithSideInput.FilterDataset(
                                      traindays, trainSetExtracted))
                              .withSideInputs(traindays))
              .apply("ToCsv", ParDo.of(new DoFn<Flight, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                      Flight f = c.element();
                      try {
                          String csv = f.toTrainingCsv();
                          c.output(csv);
                      } catch(NumberFormatException e) {
                          CreateTrainingDataset10.LOG.error(Arrays.toString(f.getFields()));
                      }
                  }
              }));
  }
}
