package org.example.flight;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.exceptions.EventMalformedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightRepository {
    final Pipeline pipeline;
    private static final Logger LOG = LoggerFactory.getLogger(FlightRepository.class);

    public FlightRepository(Pipeline p) {
        this.pipeline = p;
    }

    public PCollection<Flight> selectWhereExpectedEventOccurredBefore(String expectedEventDate) {

        return pipeline.apply("ReadLines",
                        BigQueryIO
                                .read(
                                        (SchemaAndRecord elem)->elem
                                                .getRecord()
                                                .get("EVENT_DATA")
                                                .toString())
                                .fromQuery(whereExpectedEventOccurredBefore_SQL(expectedEventDate))
                                .usingStandardSql()
                                .withCoder(StringUtf8Coder.of()))
                        .apply("toFlight", ParDo.of(new FlightHelper.ToFlightFn()));
    }

    public String whereExpectedEventOccurredBefore_SQL(String expectedEventDate) {

        return "SELECT EVENT_DATA FROM flights.simevents" +
                " WHERE FL_DATE <= '" + expectedEventDate + "'" +
                " AND (EVENT = '" + Events.WHEELSOFF.toString().toLowerCase() + "'" +
                " OR EVENT = '" + Events.DEPARTED.toString().toLowerCase() +"'" +
                " OR EVENT = '" + Events.ARRIVED.toString().toLowerCase() +"') LIMIT 1000";
    }
}
