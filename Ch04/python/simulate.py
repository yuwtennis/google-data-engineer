#!/usr/bin/env

import argparse
import datetime
import logging
import pytz
import time
from google.cloud import bigquery
from google.cloud import pubsub_v1

# Constant variable
TIME_FORMAT='%Y-%m-%d %H:%M:%S %Z'

# Probably below time format is referring to below sentence which is trying to say UTC offset.
# 4.2. Local Offsets
# Numeric offsets are calculated as "local time minus UTC".
RFC3339_TIME_FORMAT='%Y-%m-%dT%H:%m:%S-00:00'

def publish(publisher, topics, allevents, notify_time):
    timestamp = notify_time.strftime(RFC3339_TIME_FORMAT)
    for key in topics:
        topic = topics[key]
        events = allevents[key]

        # The client automatically batches
        logging.info('Publishing {} {} till {}'.format(len(events), key, timestamp))
        for event_data in events:
            # https://googleapis.github.io/google-cloud-python/latest/pubsub/publisher/index.html
            # By default, Pub/Sub accepts a maximum of 1,000 messages in a batch, 
            # and the size of a batch can not exceed 10 megabytes.
            publisher.publish(topic, event_data.encode(), EventTimeStamp=timestamp)

def notify(publisher, topics, rows, simStartTime, programStart, speedFactor):
    def compute_sleep_secs(notify_time):
        # Elapsed time since program started
        time_elapsed = (datetime.datetime.utcnow() - programStart).seconds

        # Elapsed time since simulator started in simulator's time space.
        # If the speed-up factor is 60, a 60-minute difference in event time translates to a 1-minute difference
        # in system time, and so the record should be published a minute later.
        sim_time_elapsed = (notify_time - simStartTime).seconds / speedFactor

        # If the event time clock is ahead of the system clock, we sleep for the necessary amount of time 
        # so as to allow the simulation catch up
        to_sleep_secs = sim_time_elapsed - time_elapsed

        return to_sleep_secs

    # Prepare dictionary including events which will be published to PubSub
    tonotify = {}
    for key in topics:
        tonotify[key] = list()

    for row in rows:
        event, notify_time, event_data = row

        if compute_sleep_secs(notify_time) > 1:
            # Here it means that simulator events had caught up system time.
            # Program will sleep amount of time which simulator event had went ahead.

            # notify the accumulated tonotify
            # notify_time will be the last notify_time within the batch of events
            publish(publisher, topics, tonotify, notify_time)

            # Recompute sleep, since notification takes a while
            to_sleep_secs = compute_sleep_secs(notify_time)

            if to_sleep_secs > 0:
                logging.info('Sleeping {} seconds'.format(to_sleep_secs))             
                time.sleep(to_sleep_secs)

        # Here it means simulator events are still catching up up the system time.
        tonotify[event].append(event_data)

    # Left-over records; notify again
    publish(publisher, topics, tonotify, notify_time)

def extract_and_load(startTime, endTime, speedFactor, project):
    # Initialize bigquery client object
    # https://cloud.google.com/bigquery/docs/python-client-migration

    client = bigquery.Client()
    
    querystr = """
SELECT
    EVENT,
    NOTIFY_TIME,
    EVENT_DATA
FROM
    `elite-caster-125113.flights.simevents`
WHERE
    NOTIFY_TIME >= TIMESTAMP('{}')
    AND NOTIFY_TIME < TIMESTAMP('{}')
ORDER BY NOTIFY_TIME ASC
"""

    rows = client.query(querystr.format(
                                           startTime,
                                           endTime
                       ))

    # Start time of the simulator program. Set systematically as UTC.
    programStart = datetime.datetime.utcnow()

    # Start time of the notify time of the record. Set using input argument.
    simStartTime = datetime.datetime.strptime( startTime, TIME_FORMAT ).replace(tzinfo=pytz.UTC)

    print 'Simulation start time is {}'.format(simStartTime)

    # Create topics in pubsub
    publisher = pubsub_v1.PublisherClient(
                    batch_settings=pubsub_v1.types.BatchSettings(
                                       max_messages=500
                                   )
                )
    topics = {}

    for event_type in ['departed', 'arrived']:
        topics[event_type] = publisher.topic_path(project, event_type)

        # If topic is not properly created give one more try to create a topic
        try:
            publisher.get_topic(topics[event_type])
        except:
            publisher.create_topic(topics[event_type])

    notify(publisher, topics, rows, simStartTime, programStart, speedFactor)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--startTime',   required=True, help='Example: 2015-05-01 00:00:00 UTC')
    parser.add_argument('--endTime',     required=True, help='Example: 2015-05-03 00:00:00 UTC')
    parser.add_argument('--speedFactor', required=False, default=1, type=float)
    parser.add_argument('--project',     required=True )

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
 
    extract_and_load(args.startTime, args.endTime, args.speedFactor, args.project)
