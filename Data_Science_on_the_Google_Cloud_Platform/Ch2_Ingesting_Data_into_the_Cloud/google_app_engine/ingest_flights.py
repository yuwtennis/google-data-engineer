#!/usr/bin/env

'''
File name: ingest_flights.py
Author: Yu Watanabe
Date created: Jan 13, 2019
Date last modified: Jan 13, 2019
Python Version Used: 3.7.2
Description: Automates the process of fetching flight data from BTS Website
'''

import urllib.request
import urllib.parse
import glob
import os
import zipfile
import logging
import tempfile
import shutil
import datetime
from google.cloud import storage
from google.cloud.storage import Blob

'''
This program will read data from BTS website and upload to google cloud storage

Program will consist below main procedures

1. Download data from the BTS website to a local file
2. Unzip the downloaded ZIP file and extract the CSV file it contains.
3. Remove quotes and the trailing comma from the CSV file.
4. Upload the CSV file to Google Cloud Storage.

Program is built using Python 3.7.1
'''

# Custom Exception class

# This exception class will be called when only header exists inside csv file
class DataUnavailable(Exception):
    def __init__(self, message):
        self.message = message

# This exception class will be called when there are more (or less) fields exist in each line
class UnexpectedFormat(Exception):
    def __init__(self, message):
        self.message = message

def ingest(year, month, bucket, proj):
    tmpdir = tempfile.mkdtemp(prefix='ingest_flights')

    print("Temporary Directory: {}".format(tmpdir))

    try:
        # Procedure 1
        filename = download(year, month, tmpdir)

        # Procedure 2
        bts_csv = zip_to_csv(filename, tmpdir)

        # Procedure 3
        csvfile = remove_quotes_and_commas(bts_csv, year, month)
        verify_ingest(csvfile)

        # Procedure 4
        gsloc = "flights/raw/{}".format( os.path.basename(csvfile) )
        return upload( csvfile, proj, bucket, gsloc )

    finally:
        # Cleanup of temporary directory must be done manually
        # Below must be done within finally clause because it has to be executed in any exception that occurs
        print("Cleaning up by removing {}".format(tmpdir))
        shutil.rmtree(tmpdir)

def next_month(bucketname, proj):
    # Prepare the Google cloud storage client
    client = storage.Client( proj )

    # First get the full blob list from the bucket
    bucket = client.get_bucket(bucketname)
    blobs = list(bucket.list_blobs(prefix='flights/raw/'))

    # Just pick up the files which has csv as extension
    files = [ blob.name for blob in blobs if 'csv' in blob.name ]

    # Get the last element inside the file. CSV files will be in ascending order.
    lastfile = os.path.basename( files[-1] )

    print("Last file in google storage: {}".format(lastfile))

    # Get month and year from filename
    # First 4 bytes are year
    year = lastfile[0:4]

    # Next 2 bytes are month
    month = lastfile[4:6]

    # I will use timedelta to get the 30 th day. 
    # I will start counting from 15 th of the month . This will be day 0.
    # Trick is 
    # If there is 28 days in a month then I should count from 1st of month
    # If there is 30 days in a month then I should count from 1st of month
    # If there is 31 days in a month then I should count from 2nd of month
    # So choosing 15th as day 0 will be enough
    
    dt = datetime.datetime(int(year), int(month), 15)

    dt = dt + datetime.timedelta( 30 )

    return "{}".format(dt.year), "{:02d}".format(dt.month)

def upload(csvfile, project, bucketname, blobname):
    # Prepare google cloud storage client
    client = storage.Client(project)

    # Get bucket name
    bucket = client.get_bucket(bucketname)

    # Prepare blob instance. Blob will be relative path to bucket in Google cloud storag3
    blob = storage.Blob(blobname, bucket)

    # Upload to cloud
    blob.upload_from_filename( csvfile )

    gslocation = "gs://{}/{}".format( bucketname, blobname )
    print("Uploaded {} ...".format(gslocation))

    # Make sure its uploaded to storage
    blobs = list( bucket.list_blobs( prefix=blobname ) )

    print( "List from GS... {}".format(blobs) )

    return gslocation

def verify_ingest(outfile):
    # This will be the fields which will be compared agaist the fields inside a csv file
    expected_fields="MONTH,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID"

    with open(outfile, "r") as outfp:
        # First read the very first line of the target file
        firstline = outfp.readline().strip()

        # First comparision will check the fields are expected one
        if(firstline != expected_fields):
            # If the fields are not expected one then raise exception
            msg="Got header={}, but expected={}".format(firstline, expected_fields)

            logging.error(msg)
            raise UnexpectedFormat(msg)

        # Next comparision will check the actual data exists by checking 2nd line
        if( next(outfp, None) == None):
            msg = "Received file from BTS with only header and no contents!"

            logging.error(msg)
            raise DataUnavailable(msg)

def remove_quotes_and_commas( csvfile, year, month ):
    # Use try catch finally block to deal with dirty data
    print("Target file: {}".format(csvfile))
    try:
        outfile = os.path.join( os.path.dirname(csvfile), "{}{}.csv".format(year,month) )

        with open( csvfile, "r" ) as infp:
            with open( outfile, "w" ) as outfp:
                for line in infp:
                    # Right strip the newline then right strip comma finally remove all double quotes
                    outline = line.rstrip().rstrip(',').translate( str.maketrans('', '', '"') )

                    outfp.write(outline)
                    outfp.write("\n")

        print("Cleaned file: {}".format(outfile))
    finally:
        print("... removing {}".format(csvfile))
        os.remove(csvfile)
        infp.close()
        outfp.close()

    return outfile

def zip_to_csv( filename, destdir ):
    '''
    This definition will extract zip file into currenct working directory
    '''

    # Prepare the zip object for extraction
    z = zipfile.ZipFile( filename, mode='r' )
    
    # Change working directory to where csv files exists
    os.chdir( destdir )

    # Extract the zip archive. zipfile will only extract files into current working directory.
    z.extractall()

    # Get relative path to csv file
    csvfile = os.path.join( destdir, z.namelist()[0])

    # Gracefully close the zip reference
    z.close()

    # Check that csv file is correctly placed
    print( "CSV FILE: {}".format( glob.glob(csvfile)[0] ) )

    return csvfile

def download( YEAR, MONTH, destdir):
    '''
    This definition will download on-time performance data and returns list of csv files
    '''

    url="https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=236&Has_Group=3&Is_Zipped=0"

    PARAMS="UserTableName=Reporting_Carrier_On_Time_Performance_1987_present&DBShortName=&RawDataTable=T_ONTIME_REPORTING&sqlstr=+SELECT+MONTH%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID+FROM++T_ONTIME_REPORTING+WHERE+Month+%3D1+AND+YEAR%3D2018&varlist=MONTH%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=January&timename=Month&GEOGRAPHY=All&XYEAR=2018&FREQUENCY=1&VarDesc=Year&VarType=Num&VarDesc=Quarter&VarType=Num&VarName=MONTH&VarDesc=Month&VarType=Num&VarDesc=DayofMonth&VarType=Num&VarDesc=DayOfWeek&VarType=Num&VarDesc=FlightDate&VarType=Char&VarDesc=Reporting_Airline&VarType=Char&VarDesc=DOT_ID_Reporting_Airline&VarType=Num&VarDesc=IATA_CODE_Reporting_Airline&VarType=Char&VarDesc=Tail_Number&VarType=Char&VarDesc=Flight_Number_Reporting_Airline&VarType=Char&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType=Num&VarDesc=Origin&VarType=Char&VarDesc=OriginCityName&VarType=Char&VarDesc=OriginState&VarType=Char&VarDesc=OriginStateFips&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarDesc=Dest&VarType=Char&VarDesc=DestCityName&VarType=Char&VarDesc=DestState&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc=DestStateName&VarType=Char&VarDesc=DestWac&VarType=Num&VarDesc=CRSDepTime&VarType=Char&VarDesc=DepTime&VarType=Char&VarDesc=DepDelay&VarType=Num&VarDesc=DepDelayMinutes&VarType=Num&VarDesc=DepDel15&VarType=Num&VarDesc=DepartureDelayGroups&VarType=Num&VarDesc=DepTimeBlk&VarType=Char&VarDesc=TaxiOut&VarType=Num&VarDesc=WheelsOff&VarType=Char&VarDesc=WheelsOn&VarType=Char&VarDesc=TaxiIn&VarType=Num&VarDesc=CRSArrTime&VarType=Char&VarDesc=ArrTime&VarType=Char&VarDesc=ArrDelay&VarType=Num&VarDesc=ArrDelayMinutes&VarType=Num&VarDesc=ArrDel15&VarType=Num&VarDesc=ArrivalDelayGroups&VarType=Num&VarDesc=ArrTimeBlk&VarType=Char&VarDesc=Cancelled&VarType=Num&VarDesc=CancellationCode&VarType=Char&VarDesc=Diverted&VarType=Num&VarDesc=CRSElapsedTime&VarType=Num&VarDesc=ActualElapsedTime&VarType=Num&VarDesc=AirTime&VarType=Num&VarDesc=Flights&VarType=Num&VarDesc=Distance&VarType=Num&VarDesc=DistanceGroup&VarType=Num&VarDesc=CarrierDelay&VarType=Num&VarDesc=WeatherDelay&VarType=Num&VarDesc=NASDelay&VarType=Num&VarDesc=SecurityDelay&VarType=Num&VarDesc=LateAircraftDelay&VarType=Num&VarDesc=FirstDepTime&VarType=Char&VarDesc=TotalAddGTime&VarType=Num&VarDesc=LongestAddGTime&VarType=Num&VarDesc=DivAirportLandings&VarType=Num&VarDesc=DivReachedDest&VarType=Num&VarDesc=DivActualElapsedTime&VarType=Num&VarDesc=DivArrDelay&VarType=Num&VarDesc=DivDistance&VarType=Num&VarDesc=Div1Airport&VarType=Char&VarDesc=Div1AirportID&VarType=Num&VarDesc=Div1AirportSeqID&VarType=Num&VarDesc=Div1WheelsOn&VarType=Char&VarDesc=Div1TotalGTime&VarType=Num&VarDesc=Div1LongestGTime&VarType=Num&VarDesc=Div1WheelsOff&VarType=Char&VarDesc=Div1TailNum&VarType=Char&VarDesc=Div2Airport&VarType=Char&VarDesc=Div2AirportID&VarType=Num&VarDesc=Div2AirportSeqID&VarType=Num&VarDesc=Div2WheelsOn&VarType=Char&VarDesc=Div2TotalGTime&VarType=Num&VarDesc=Div2LongestGTime&VarType=Num&VarDesc=Div2WheelsOff&VarType=Char&VarDesc=Div2TailNum&VarType=Char&VarDesc=Div3Airport&VarType=Char&VarDesc=Div3AirportID&VarType=Num&VarDesc=Div3AirportSeqID&VarType=Num&VarDesc=Div3WheelsOn&VarType=Char&VarDesc=Div3TotalGTime&VarType=Num&VarDesc=Div3LongestGTime&VarType=Num&VarDesc=Div3WheelsOff&VarType=Char&VarDesc=Div3TailNum&VarType=Char&VarDesc=Div4Airport&VarType=Char&VarDesc=Div4AirportID&VarType=Num&VarDesc=Div4AirportSeqID&VarType=Num&VarDesc=Div4WheelsOn&VarType=Char&VarDesc=Div4TotalGTime&VarType=Num&VarDesc=Div4LongestGTime&VarType=Num&VarDesc=Div4WheelsOff&VarType=Char&VarDesc=Div4TailNum&VarType=Char&VarDesc=Div5Airport&VarType=Char&VarDesc=Div5AirportID&VarType=Num&VarDesc=Div5AirportSeqID&VarType=Num&VarDesc=Div5WheelsOn&VarType=Char&VarDesc=Div5TotalGTime&VarType=Num&VarDesc=Div5LongestGTime&VarType=Num&VarDesc=Div5WheelsOff&VarType=Char&VarDesc=Div5TailNum&VarType=Char"

    # Urlopen in Python3 supports bytes for "data"
    response = urllib.request.urlopen( url, PARAMS.encode('ascii') )

    # Prepare filename to download the csv files
    filename = os.path.join(destdir, "{}{}.zip".format(YEAR,MONTH))

    # Download the file
    with open(filename, "wb") as fp:
        fp.write( response.read()  )

    # Check that the downloaded zip file is there
    print( "ZIP FILE: {}".format( glob.glob(filename)[0] ) )

    return filename

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='ingest flights data from BTS website to GCS')

    parser.add_argument( '--bucket', help='GCS bucket to upload data to.', required=True )
    parser.add_argument( '--year',   help='Example: 2015', required=False )
    parser.add_argument( '--month',  help='Example: 01', required=False )
    parser.add_argument( '--proj',   help='Name of the project', required=True )

    try:
        args = parser.parse_args() 

        if args.year is None or args.month is None:
           print("Neither year nor month specified. Calculating year and month automatically...")
           year, month = next_month(args.bucket, args.proj)
        else:
           year = args.year
           month = args.month

        gcsfile = ingest( year, month, args.bucket, args.proj )
    except DataUnavailable as e:
        print ('Try again later: {}'.format(e.message))
