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
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.entities.Flight;
import org.example.transforms.GroupAndCombine;
import org.example.transforms.MutatingTheFlightObject;
import org.example.transforms.ParsingIntoObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CreateTrainingDataset6 {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset6.class);

  private static final String DELAY_TYPE_DEPARTURE = "DEPARTURE";


  public static void main(String[] args) {

      List<String> events = Arrays.asList(
              "2018-01-02,AA,19805,AA,2305,11298,1129806,30194,DFW,14683,1468305,33214,SAT,2018-01-02 06:01:00,2018-01-02 06:11:00,10.00,,,,,2018-01-02 07:11:00,,,0.00,,,247.00,32.89722222,-97.03777778,-21600.0,29.53388889,-98.46916667,-21600.0,departed,2018-01-02 06:11:00",
              "2018-01-02,AA,19805,AA,2305,11298,1129806,30194,DFW,14683,1468305,33214,SAT,2018-01-02 06:01:00,2018-01-02 06:11:00,10.00,,,,,2018-01-02 07:11:00,,,0.00,,,247.00,32.89722222,-97.03777778,-21600.0,29.53388889,-98.46916667,-21600.0,departed,2018-01-02 06:11:00",
              "2018-01-02,AA,19805,AA,1922,11057,1105703,31057,CLT,14492,1449202,34492,RDU,2018-01-02 05:30:00,2018-01-02 05:21:00,-9.00,18.00,2018-01-02 05:39:00,2018-01-02 06:07:00,5.00,2018-01-02 06:19:00,2018-01-02 06:12:00,-7.00,0.00,,0.00,130.00,35.21361111,-80.94916667,-18000.0,35.87777778,-78.78750000,-18000.0,arrived,2018-01-02 06:12:00"
      );

      Pipeline p = Pipeline.create();

      PCollection<Flight> flights = p.apply(Create.of(events))
              .apply("ParseFlights", ParDo.of(new ParsingIntoObjects.ParseFlightsFn()))
              .apply("GoodFlights", ParDo.of(new ParsingIntoObjects.GoodFlightsFn()));

      // Airport/Delay Map
      PCollectionView<Map<String, Double>> avgDelay = flights
              .apply("AvgDepHours", ParDo.of(new GroupAndCombine.AirportHourFn()))
              .apply(Mean.perKey())
              .apply(View.asMap());

      // Load Avg Departure Delay
      flights.apply("CloneAndSetAvgDelay",
                      ParDo.of(new MutatingTheFlightObject.AddDelayInfoFn(DELAY_TYPE_DEPARTURE, avgDelay))
                              .withSideInputs(avgDelay))
              .apply("Log", ParDo.of(new DoFn<Flight, Void>(){
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                      Flight f = c.element();
                      LOG.info(f.toTrainingCsv());
                  }
              }));

      p.run();
  }
}
