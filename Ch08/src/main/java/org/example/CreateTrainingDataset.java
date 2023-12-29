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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class CreateTrainingDataset {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset.class);

  public static void main(String[] args) {
    List<String> events = Arrays.asList(
            "2018-01-02,AA,19805,AA,102,12173,1217305,32134,HNL,11298,1129806,30194,DFW,2018-01-03 07:00:00,2018-01-03 08:03:00,63.00,24.00,2018-01-03 08:27:00,2018-01-03 15:00:00,4.00,2018-01-03 14:22:00,2018-01-03 15:04:00,42.00,0.00,,0.00,3784.00,21.31777778,-157.92027778,-36000.0,32.89722222,-97.03777778,-21600.0,arrived,2018-01-03 15:04:00",
            "2018-01-02,AA,19805,AA,1391,13303,1330303,32467,MIA,15024,1502403,34945,STT,2018-01-02 17:00:00,2018-01-02 16:59:00,-1.00,15.00,2018-01-02 17:14:00,2018-01-03 15:30:00,4.00,2018-01-03 15:43:00,2018-01-03 15:34:00,-9.00,0.00,,0.00,1107.00,25.79527778,-80.29000000,-18000.0,18.33722222,-64.97333333,0.0,arrived,2018-01-03 15:34:00",
            "2018-01-02,AA,19805,AA,1391,13303,1330303,32467,MIA,15024,1502403,34945,STT,2018-01-02 17:00:00,2018-01-02 16:59:00,-1.00,15.00,2018-01-02 17:14:00,2018-01-03 15:30:00,4.00,2018-01-03 15:43:00,2018-01-03 15:34:00,-9.00,0.00,,0.00,1107.00,25.79527778,-80.29000000,-18000.0,18.33722222,-64.97333333,0.0,arrived,2018-01-03 15:34:00"
    );

    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    p.apply(Create.of(events))
            .apply(ParDo.of(new DoFn<String, String>(){
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                String input = c.element();
                if (input.contains("MIA")) {
                  c.output(input);
                }
              }
            }))
            .apply(ParDo.of(new DoFn<String, Void>(){
              @ProcessElement
              public void processElement(ProcessContext c) {
                LOG.info(c.element());
              }
            }));

    p.run();
  }
}