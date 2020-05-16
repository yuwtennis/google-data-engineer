#!/usr/bin/env
#
# Self study code built for Data Science on the Google Cloud Platform Ch 4
# apache beam documentation
# https://beam.apache.org/documentation/programming-guide/#pipeline-io
#
# Reference guide
# https://beam.apache.org/releases/pydoc/2.11.0/
#
# This script will parse fields from csv file downloaded from below site.
# https://www.transtats.bts.gov/TableInfo.asp

import argparse
import sys
import csv
import apache_beam as beam
from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions

class PrintFn(beam.DoFn):
    def process(self, element):
        # Just print
        print element
        return

def format(s):
    ( word, count ) = s

    return "word: {0} count: {1}".format(word.encode('utf-8'), count)

def run_pipeline(known_args, pipeline_args):
    # Simple process for apache beam pipeline
    # This code will run the pipeline in GCP DataFlow 
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(runner='DataFlowRunner', options=options) as p:
        #
        # Pipeline(0): Data ingestion
        #
        # "lines" will include pcollections of each line
        # Options
        # file_pattern: File path to file
        # skip_header_lines: First line will be skipped. Set to "1".

        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.ReadFromText
        collections = p | ReadFromText( file_pattern=known_args.input, skip_header_lines=1 )

        #
        # Pipeline(n): Detailed Transformation
        # 1. Parse each line and return fields as a list. Use csv module to remove any double quotes inside field
        # 2. Just get "AIRPORT_SEQ_ID"(0),"LATITUDE"(21),"LONGITUDE"(26)
        #
        airports = (
                       collections
                       | 'Extract_Into_Fields' >> beam.Map( lambda x: next(csv.reader([x],delimiter=',')))
                       | 'Set_Fields' >> beam.Map(lambda x: (x[0],(x[21],x[26])))
                   )

        #
        # Pipeline(Final)
        #
        # Write results to a file. Tuples are unpacked while function call.
        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        (
            airports 
            | beam.Map(lambda (airport,data): "{0},{1}".format(airport,','.join(data)))
            | WriteToText( file_path_prefix = known_args.output )
        )

def main():
    # Setting required arguments for data flow
    # https://cloud.google.com/dataflow/docs/guides/specifying-exec-params

    parser = argparse.ArgumentParser()
    parser.add_argument('--input')
    parser.add_argument('--output')

    # Options required to run pipeline in data flow is below
    # https://beam.apache.org/documentation/runners/dataflow/
    known_args, pipeline_args = parser.parse_known_args(sys.argv[1:])

    run_pipeline(known_args, pipeline_args)

if __name__ == "__main__":
    main()
