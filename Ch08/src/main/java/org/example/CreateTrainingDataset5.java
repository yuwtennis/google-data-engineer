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
import org.apache.beam.sdk.values.PCollection;
import org.example.transforms.GroupAndCombine;
import org.example.transforms.ParsingIntoObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTrainingDataset5 {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset5.class);

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

      PCollection<String> bqRowAsCsv =
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

      bqRowAsCsv.apply("ToTrainCsv", new ParsingIntoObjects.ParsingIntoObjectsTransform())
            .apply("WriteFlights",
                    TextIO
                            .write()
                            .to(options.getOutput()+"flights5")
                            .withSuffix(".csv")
                            .withoutSharding());

      bqRowAsCsv.apply("GroupAndCombine", new GroupAndCombine.ComputeTimeAvgTransform())
            .apply("WriteAvgHourCsv",
                    TextIO
                            .write()
                            .to(options.getOutput()+"delays5")
                            .withSuffix(".csv")
                            .withoutSharding());
      p.run();
  }
}
