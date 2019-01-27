#!/usr/bin/env

'''
File name: flights.py
Author: Yu Watanabe
Date created: Jan 13, 2019
Date last modified: Jan 13, 2019
Python Version Used: 3.7.2
Description: Implements WSGI using Flask
'''

from flask import Flask
import flask
import ingest_flights
import logging

app = Flask(__name__)

# If the Request URI is "https://HOST:5000/ingest then request will be routed here
@app.route('/ingest')

def ingest_next_month():

    try:

        # Verify that this is a cron job request
        is_cron = flask.request.headers['X-Appengine-Cron']
        logging.info('Received cron request {}'.format(is_cron))

        # CLOUD_STORAGE_BUCKET will be defined in app.yaml
        bucket = CLOUD_STORAGE_BUCKET

        # Runtime environment variable from google app engine
        project = GOOGLE_CLOUD_PROJECT

        year, month = ingest_flights.next_month( bucket, project )

        gcsfile = ingest( year, month, bucket, project )

        status = "Successfully ingested {}".format(gcsfile)
    except KeyError as e:
        logging.info('Rejected non-Cron request')

    except ingest_flights.DataUnavailable as e:
        status = "File for {}.{} not available yet ...".format( year, month )

    logging.info( status )
    return status
