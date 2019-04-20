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
# 3. Using [2], pipeline will convert the dep/arr time to UTC time

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

        # If timezone_at returns no timezone , then use UTC
        if tz is None:
            tz = 'UTC'
        return (lat, lon, tz)
    except ValueError:
        # the coordinates were out of bounds
        return (lat, lon, 'TIMEZONE')

def as_utc(date, hhmm, tzone):
    if len(hhmm) and tzone is no None:  
        # Import modules at this level instead globally
        import datetime, pytz

    return

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
           fields[f] = as_utc( fields[0], fields[f], dep_timezone) # FL_DATE, f, timezone

       for f in [18, 20, 21]: # WHEELS_ON, CRS_ARR_TIME, ARR_TIME
           fields[f] = as_utc( fields[0], fields[f], dep_timezone) # FL_DATE, f, timezone

    # Use python generator instead of returning list(iterable) to save memory
    yield ',".join(fields)

def format(s):
    ( word, count ) = s

    return "word: {0} count: {1}".format(word.encode('utf-8'), count)

def run_pipeline(in_file, out_file):
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
        collections = p | ReadFromText( file_pattern=in_file, skip_header_lines=1 )

        #
        # Pipeline(n): Detailed Transformation
        # Final PCollection will be used as side input for the date time convertion in the next transformation
        # 1. Parse each line and return fields as a list. Use csv module to remove any double quotes inside field
        # 2. Filter out invalid fields
        # 3. Just get "AIRPORT_SEQ_ID"(0),"LATITUDE"(21),"LONGITUDE"(26). Also add timezone for correspondng coordinates
        #
        airports = (
                       collections
                       | 'Extract' >> beam.Map(lambda x: next(csv.reader([x],delimiter=',')))
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
