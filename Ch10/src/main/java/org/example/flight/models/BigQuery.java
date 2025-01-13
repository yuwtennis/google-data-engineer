package org.example.flight.models;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.example.flight.Flight;
import org.example.flight.FlightPred;

import java.util.ArrayList;
import java.util.List;

public class BigQuery {
    public static String[] FLIGHTBQTYPES = new String[]{
            "DATE" ,"STRING", "STRING", "STRING", "STRING",
            "STRING","INTEGER","STRING","STRING","STRING",
            "INTEGER","STRING","STRING","TIMESTAMP","TIMESTAMP","FLOAT","FLOAT",
            "TIMESTAMP","TIMESTAMP","FLOAT","TIMESTAMP","TIMESTAMP","FLOAT","STRING","STRING",
            "STRING","FLOAT","FLOAT","FLOAT","FLOAT","FLOAT",
            "FLOAT","FLOAT","STRING","TIMESTAMP"
    };

    public static TableSchema createBqSchema() {
        List<TableFieldSchema> fieldSchemas = new ArrayList<>();

        for (Flight.INPUTCOLS col : Flight.INPUTCOLS.values()) {
            fieldSchemas.add(new TableFieldSchema()
                    .setName(col.toString().toLowerCase())
                    .setType(FLIGHTBQTYPES[col.ordinal()])
            );
        }

        fieldSchemas.add(new TableFieldSchema().setName("ONTIME").setType("FLOAT"));

        return new TableSchema().setFields(fieldSchemas);
    }

    public static TableRow createTableRow(FlightPred fp) {
        TableRow row = new TableRow();

        for (Flight.INPUTCOLS col : Flight.INPUTCOLS.values()) {
            if(fp.getFlight().getField(col).isEmpty()) {
                row.set(
                        col.toString(),
                        null);
            } else if(FLIGHTBQTYPES[col.ordinal()].equals("INTEGER")) {
                row.set(
                        col.toString(),
                        Integer.getInteger(fp.getFlight().getField(col)));
            } else if(FLIGHTBQTYPES[col.ordinal()].equals("FLOAT")) {
                row.set(
                        col.toString(),
                        fp.getFlight().getFieldAsFloat(col));
            } else {
                row.set(
                        col.toString(),
                        fp.getFlight().getField(col));
            }
        }

        // ontime
        row = row.set("ONTIME", (float) fp.getOntime());

        return row;
    }

    public static class FlightPredToTableRowFn extends DoFn<FlightPred, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) { c.output(BigQuery.createTableRow(c.element())); }
    }
}
