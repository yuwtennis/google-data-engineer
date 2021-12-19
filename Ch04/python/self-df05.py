#!/usr/bin/env
'''
File name: self-df05.py
Author: Yu Watanabe
Date created: Apr 29, 2019
Date last modified: Apr 29, 2019
Python Version Used: 2.7.15
Description: Takes flight data and corrects the local date time to UTC timezone
History:
  self-df05.py: Changed the runner from "DirectRunner" to "DataFlowRunner"
'''

# ToDo Refactor

#
# Self study code built for Data Science on the Google Cloud Platform Ch 4
# apache beam documentation
# Source code is written to execute on DataFlow.
# https://beam.apache.org/documentation/programming-guide/#pipeline-io
#
# Reference guide
# https://beam.apache.org/releases/pydoc/2.11.0/
#
# 1. This script will parse fields from csv file downloaded from below site.
# https://www.transtats.bts.gov/TableInfo.asp
# 2. Parse lat and lon and find the location
# 3. Using [2], pipeline will convert the dep/arr time to UTC time

import csv
import datetime, pytz
import argparse
import logging

from timezonefinder import TimezoneFinder

import apache_beam as beam

# Globally define logging object
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z', level=logging.INFO)

def addtimezone(lat, lon):
    # Code is based on sample on below page
    # https://pypi.org/project/timezonefinder/
    try:
        tf = TimezoneFinder()
        tz = tf.timezone_at(lng=float(lon), lat=float(lat))

        # If timezone_at returns no timezone , then use UTC
        if tz is None:
            tz = 'UTC'
        return (lat, lon, tz)
    except ValueError:
        # the coordinates were out of bounds
        return (lat, lon, 'TIMEZONE')

def add_24h_if_before(arrtime, deptime):
    if len(deptime) > 0 and len(arrtime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, '%Y-%m-%d %H:%M:%S')
        adt += datetime.timedelta(hours=24)
        return adt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return  arrtime

def as_utc(date, hhmm, tzone):
    try:
        if len(hhmm) and tzone is not None:  

            # Prepare pytz instance using the timezone info
            loc_tz = pytz.timezone(tzone)

            # From the pytz instance create datetime instance parsing the input date.
            # "is_dst" is set false since it is not dealing with Daylight Saving Time
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, "%Y-%m-%d"), is_dst=False)

            # loc_dt will have 00:00:00 (set timezone). We will need to fastforward the hour and minutes.
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]),
                                     minutes=int(hhmm[2:]))

            # Finally convert the time to utc
            utc_dt = loc_dt.astimezone(pytz.utc)

            return utc_dt.strftime("%Y-%m-%d %H:%M:%S"), loc_dt.utcoffset().total_seconds()
        else:   
            return '', 0 # empty strings corresponds to canceled flights
    except ValueError as e:
        # If something is wrong ,ValueError will be raised from "datetime.datetiem.strptime"
        print('{}.{}.{}'.format(date, hhmm, tzone))

        raise e

def tz_correct(line, airport_timezones):
    # This function will find the timezone of that airport and convert it into UTC
    # using ORIGIN_AIRPORT_SEQ_ID(depart) and DEST_AIRPORT_SEQ_ID(arrive)

    fields = line.split(',')

    # Fields validation
    # Timezone convertion will be done on valid fields not header line.
    if fields[0] != 'FL_DATE' and len(fields):
       dep_airport_id = fields[6]  # Set ORIGIN_AIRPORT_SEQ_ID
       arr_airport_id = fields[10] # Set DEST_AIRPORT_SEQ_ID

       dep_timezone = airport_timezones[dep_airport_id][2] # Set timezone of the departure airport
       arr_timezone = airport_timezones[arr_airport_id][2] # Set timezone of the arrival airport

       # Convert all times to UTC and replace the original lines
       for f in [13, 14, 17]: # CRS_DEP_TIME, DEP_TIME, WHEELS_OFF 
           fields[f], deptz = as_utc( fields[0], fields[f], dep_timezone) # FL_DATE, f, timezone

       for f in [18, 20, 21]: # WHEELS_ON, CRS_ARR_TIME, ARR_TIME
           fields[f], arrtz = as_utc( fields[0], fields[f], arr_timezone) # FL_DATE, f, timezone

       # Fast forward the arrival time if the departure time is later than arrival time
       for f in [17, 18, 20, 21]:
           fields[f] = add_24h_if_before(fields[f], fields[14]) # arrival time , departure time

       # Add the timezone of departure and arrival airport 
       fields.extend(airport_timezones[dep_airport_id])
       fields[-1]=str(deptz)
       fields.extend(airport_timezones[arr_airport_id])
       fields[-1]=str(arrtz)

       yield fields

def get_next_event(fields):
    # Generate departed events
    if len(fields[14]) > 0:
        # Fields in original list must not be modified!
        event=list(fields)

        # Departure event will have string "departed" and "notification time"
        event.extend(["departed", fields[14]])

        # Null out the events that are not available at departure time
        # Unavailable events at departure time are:
        # 16. TAXI_OUT
        # 17. WHEELS_OFF
        # 18. WHEELS_ON
        # 19. TAXI_IN
        # 21. ARR_TIME
        # 22. ARR_DELAY
        # 25. DIVERTED
        for f in [16,17,18,19,21,22,25]:
            event[f]=''

        # First yield. Up to this line will be executed when this function is called.
        yield event

    # Generate arrived events
    if len(fields[21]) > 0:
        event=list(fields)

        # Arrived event will have string "arrived"
        event.extend(["arrived",fields[21]])

        # Second yield. This generator will be called after the first generator is processed.
        yield event

def create_row(fields):
    header = 'FL_DATE,OP_UNIQUE_CARRIER,OP_CARRIER_AIRLINE_ID,OP_CARRIER,OP_CARRIER_FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE,DEP_AIRPORT_LAT,DEP_AIRPORT_LON,DEP_AIRPORT_TZOFFSET,ARR_AIRPORT_LAT,ARR_AIRPORT_LON,ARR_AIRPORT_TZOFFSET,EVENT,NOTIFY_TIME'.split(',')
    
    featdict={}

    for name, value in zip(header, fields):
        featdict[name] = value

    featdict['EVENT_DATA'] = ','.join(fields)

    return featdict

def run_pipeline(project, bucket, dataset):
    # Prepare the arguments for Cloud Data Flow
    param = [
      '--project={0}'.format(project),
      '--job_name=ch04timecorr',
      '--save_main_session',
      '--staging_location=gs://{0}/flights/staging/'.format(bucket),
      '--temp_location=gs://{0}/flights/temp/'.format(bucket),
      '--requirements_file=./requirements.txt',
      '--max_num_workers=10',
      '--autoscaling_algorithm=THROUGHPUT_BASED',
      '--runner=DataflowRunner'
    ]

    # All input files are on Google Storage
    airports_file_name = "gs://{0}/flights/sideinput/89598257_T_MASTER_CORD.csv.gz".format(bucket), # List of airports
    flights_raw_file = "gs://{0}/flights/raw/*.csv".format(bucket) # Historical Data

    flights_output = "gs://{0}/flights/tzcorr/all_flights".format(bucket)

    bq_table = '{0}:{1}.simevents'.format(project, dataset)

    # Simple process for apache beam pipeline using Google Data Flow
    options=beam.options.pipeline_options.PipelineOptions(param)

    with beam.Pipeline(options=options) as p:
        #
        # Pipeline(0): Data ingestion
        #
        # "lines" will include pcollections of each line
        # Options
        # file_pattern: File path to file
        # skip_header_lines: First line will be skipped. Set to "1".

        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.ReadFromText
        collections = p | 'airports:read' >> beam.io.textio.ReadFromText(file_pattern=airports_file_name, skip_header_lines=1)

        #
        # Pipeline(1): Create side input
        # Final PCollection will be used as side input for the date time convertion in the next transformation
        # 1. Parse each line and return fields as a list. Use csv module to remove any double quotes inside field
        # 2. Filter out invalid fields
        # 3. Just get "AIRPORT_SEQ_ID"(0),"LATITUDE"(21),"LONGITUDE"(26). Also add timezone for correspondng coordinates
        #
        airports = ( collections
                       | 'airports:extract' >> beam.Map(lambda x: next(csv.reader([x],delimiter=',')))
                       | 'airports:filter' >> beam.Filter( lambda x: x[21] and x[26] )
                       | 'airports:timezone' >> beam.Map(lambda x: (x[0], addtimezone(x[21],x[26])))
                   )

        #
        # Pipeline(2): Correct timezone
        # 1. Read flight data
        # 2. Convert times into UTC
        flights = (p
                      | 'flights:read' >> beam.io.textio.ReadFromText( file_pattern=flights_raw_file, skip_header_lines=1)
                      | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
                  )

        # Write results to a file. Tuples are unpacked while function call.
        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        (flights
            | 'flights:tostring' >> beam.Map(lambda fields: ','.join(fields))
            | 'flights:out' >> beam.io.textio.WriteToText(file_path_prefix=flights_output)
        )

        # Pipeline(3): Generate departed and arrived events
        # 1. 
        events = flights | 'flights:events' >> beam.FlatMap(get_next_event)

        # Pipeline(4): Pipeline to insert rows into BigQuery
        schema = 'FL_DATE:date,OP_UNIQUE_CARRIER:string,OP_CARRIER_AIRLINE_ID:string,OP_CARRIER:string,OP_CARRIER_FL_NUM:string,ORIGIN_AIRPORT_ID:string,ORIGIN_AIRPORT_SEQ_ID:integer,ORIGIN_CITY_MARKET_ID:string,ORIGIN:string,DEST_AIRPORT_ID:string,DEST_AIRPORT_SEQ_ID:integer,DEST_CITY_MARKET_ID:string,DEST:string,CRS_DEP_TIME:timestamp,DEP_TIME:timestamp,DEP_DELAY:float,TAXI_OUT:float,WHEELS_OFF:timestamp,WHEELS_ON:timestamp,TAXI_IN:float,CRS_ARR_TIME:timestamp,ARR_TIME:timestamp,ARR_DELAY:float,CANCELLED:string,CANCELLATION_CODE:string,DIVERTED:string,DISTANCE:float,DEP_AIRPORT_LAT:float,DEP_AIRPORT_LON:float,DEP_AIRPORT_TZOFFSET:float,ARR_AIRPORT_LAT:float,ARR_AIRPORT_LON:float,ARR_AIRPORT_TZOFFSET:float,EVENT:string,NOTIFY_TIME:timestamp,EVENT_DATA:string'

        (events
            | 'events:totablerow' >> beam.Map(lambda fields: create_row(fields))
            | 'events:bq' >> beam.io.Write(beam.io.gcp.bigquery.BigQuerySink(
                                                                       bq_table,      # table
                                                                       schema=schema, # schema
                                                                       write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE,   # BigQuery Disposition
                                                                       create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED # BigQuery Disposition
                                  ))
        )

        #
        # Pipeline(Final)
        #
        # Write results to a file. Tuples are unpacked while function call.
        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        (events
            | 'flights:fileoutput' >> beam.io.textio.WriteToText(file_path_prefix = events_output)
        )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--project', required=True,  help='Unique ProjectID')
    parser.add_argument('-b', '--bucket',  required=True,  help='Bucket where your data were ingested in Chapter 2')
    parser.add_argument('-d', '--dataset', required=False, help='BigQuery dataset', default='flights')

    args=vars(parser.parse_args())
    run_pipeline(project=args['project'], bucket=args['bucket'], dataset=args['dataset'])

if __name__ == "__main__":
    main()
