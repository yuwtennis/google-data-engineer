#!/usr/bin/env
#
# Self study code built for Data Science on the Google Cloud Platform Ch 4
# apache beam documentation
# https://beam.apache.org/documentation/programming-guide/#pipeline-io
#
# Reference guide
# https://beam.apache.org/releases/pydoc/2.11.0/
#
# 1. This script will parse fields from csv file downloaded from below site.
# https://www.transtats.bts.gov/TableInfo.asp
# 2. Parse lat and lon and find the location

import csv
import apache_beam as beam
from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import WriteToText

class PrintFn(beam.DoFn):
    def process(self, element):
        # Just print
        print element
        return

def addtimezone(lat, lon):
    # Code is based on sample on below page
    # https://pypi.org/project/timezonefinder/
    try:
        from timezonefinder import TimezoneFinder

        tf = TimezoneFinder()
        tz = tf.timezone_at(lng=float(lon), lat=float(lat))

        if tz is None:
            tz = 'UTC'
        return (lat, lon, tz)
    except ValueError:
        # the coordinates were out of bounds
        return (lat, lon, 'TIMEZONE')

def format(s):
    ( word, count ) = s

    return "word: {0} count: {1}".format(word.encode('utf-8'), count)

def run_pipeline(in_file, out_file):
    # Simple process for apache beam pipeline
    with beam.Pipeline(runner='DirectRunner') as p:
        #
        # Pipeline(0): Data ingestion
        #
        # "lines" will include pcollections of each line
        # Options
        # file_pattern: File path to file
        # skip_header_lines: First line will be skipped. Set to "1".

        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.ReadFromText
        collections = p | ReadFromText( file_pattern=in_file, skip_header_lines=1 )

        #
        # Pipeline(n): Detailed Transformation
        # 1. Parse each line and return fields as a list. Use csv module to remove any double quotes inside field
        # 2. Filter out invalid fields
        # 3. Just get "AIRPORT_SEQ_ID"(0),"LATITUDE"(21),"LONGITUDE"(26). Also add timezone for correspondng coordinates
        #
        airports = (
                       collections
                       | 'Extract' >> beam.Map( lambda x: next(csv.reader([x],delimiter=',')))
                       | 'Filter' >> beam.Filter( lambda x: x[21] and x[26] )
                       | 'Timezone' >> beam.Map(lambda x: (x[0], addtimezone(x[21],x[26])))
                   )

        #
        # Pipeline(Final)
        #
        # Write results to a file. Tuples are unpacked while function call.
        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        (
            airports 
            | beam.Map(lambda (airport,data): "{0},{1}".format(airport,','.join(data)))
            | WriteToText( file_path_prefix = out_file )
        )

def main():
    in_file = "./89598257_T_MASTER_CORD.csv"
    out_file = "/tmp/airport.csv"

    run_pipeline(in_file, out_file)

if __name__ == "__main__":
    main()
