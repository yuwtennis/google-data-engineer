#!/usr/bin/env
'''
File name: self-df04.py
Author: Yu Watanabe
Date created: Apr 13, 2019
Date last modified: Jan 20, 2019
Python Version Used: 2.7.15
Description: Takes flight data and corrects the local date time to UTC timezone
'''

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

def addtimezone(lat, lon):
    # Code is based on sample on below page
    # https://pypi.org/project/timezonefinder/
    try:
        from timezonefinder import TimezoneFinder

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
    import datetime

    if deptime > 0 and arrtime > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime( arrtime, '%Y-%m-%d %H:%M:%S')
        adt += datetime.timedelta(hours=24)
        return adt.strftime('%Y-%m-%d %H:%M:%S')

    else:
        return  arrtime

def as_utc(date, hhmm, tzone):
    try:
        if len(hhmm) and tzone is not None:  
            # Due to nature of data flow import modules at this level instead globally
            import datetime, pytz

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
        print '{}.{}.{}'.format(date, hhmm, tzone)

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
           fields[f] = add_24h_if_before( fields[f], fields[14]) # arrival time , departure time

       # Add the timezone of departure and arrival airport 
       fields.extend(airport_timezones[dep_airport_id])
       fields[-1]=str(deptz)
       fields.extend(airport_timezones[arr_airport_id])
       fields[-1]=str(arrtz)

       # Use python generator instead of returning list(iterable) to save memory
       yield ','.join(fields)

def get_next_event(fields):
    # Generate departed events
    if len(fields[14]) > 0:
        # Fields in original list must not be modified!
        event=fields.split(',') 

        # Departure event will have string "departed"
        event.extend(["departed"])

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
        yield ','.join(event)

    # Generate arrived events
    if len(fields[21]) > 0:
        event=fields.split(',')

        # Arrived event will have string "arrived"
        event.extend(["arrived"])

        # Second yield. This generator will be called after the first generator is processed.
        yield ','.join(event)

def format(s):
    ( word, count ) = s

    return "word: {0} count: {1}".format(word.encode('utf-8'), count)

def run_pipeline(in_file):
    import csv

    import apache_beam as beam
    from apache_beam.io.textio import ReadFromText
    from apache_beam.io.textio import WriteToText

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
        collections = p | 'ReadAiportInfo' >> ReadFromText( file_pattern=in_file[0], skip_header_lines=1 )

        #
        # Pipeline(1): Create side input
        # Final PCollection will be used as side input for the date time convertion in the next transformation
        # 1. Parse each line and return fields as a list. Use csv module to remove any double quotes inside field
        # 2. Filter out invalid fields
        # 3. Just get "AIRPORT_SEQ_ID"(0),"LATITUDE"(21),"LONGITUDE"(26). Also add timezone for correspondng coordinates
        #
        airports = (
                       collections
                       | 'airports:Extract' >> beam.Map(lambda x: next(csv.reader([x],delimiter=',')))
                       | 'airports:Filter' >> beam.Filter( lambda x: x[21] and x[26] )
                       | 'airports:Timezone' >> beam.Map(lambda x: (x[0], addtimezone(x[21],x[26])))
                   )

        #
        # Pipeline(2): Correct timezone
        # 1. Read flight data
        # 2. Convert times into UTC
        flights = ( 
                   p | 'flights:read' >> ReadFromText( file_pattern=in_file[1], skip_header_lines=1)
                     | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
                  )

        # Write results to a file. Tuples are unpacked while function call.
        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        ( flights | 'flights:out' >> WriteToText( file_path_prefix = 'flights' ) )

        # Pipeline(3): Generate departed and arrived events
        # 1. 
        events = flights | '' >> beam.FlatMap(get_next_event)

        #
        # Pipeline(Final)
        #
        # Write results to a file. Tuples are unpacked while function call.
        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        ( events | 'event:out' >> WriteToText( file_path_prefix = 'events' ) )

def main():
    import os

    data_dir = os.getcwd() + '/../data'
    in_file = [
                  "{}/{}".format(data_dir, "89598257_T_MASTER_CORD.csv"),
                  "{}/{}".format(data_dir, "201903_part.csv")
              ]

    run_pipeline(in_file)

if __name__ == "__main__":
    main()
