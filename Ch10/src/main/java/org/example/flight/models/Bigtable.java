package org.example.flight.models;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.example.flight.Flight;
import org.example.flight.FlightPred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Bigtable {
    private static final Logger LOG = LoggerFactory.getLogger(Bigtable.class);
    private static final String CF_FAMILY = "FL";

    /***
     *
     * @param mutations
     * @param colName
     * @param colValue
     * @param ts_in_micros
     */
    private static void addCell(
            List<Mutation> mutations, String colName, String colValue, Long ts_in_micros) {
        Mutation m = null;
        if (!colValue.isEmpty()) {
            m = Mutation
                    .newBuilder()
                    .setSetCell(
                            Mutation.SetCell.newBuilder()
                                    .setColumnQualifier(ByteString.copyFromUtf8(colName))
                                    .setFamilyName(CF_FAMILY)
                                    .setValue(ByteString.copyFromUtf8(colValue))
                                    .setTimestampMicros(ts_in_micros)
                    ).build();
            mutations.add(m);
        }
    }

    public static class FlightPredToMutationFn extends DoFn<FlightPred, KV<ByteString, Iterable<Mutation>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            FlightPred fp = c.element();
            Long ts_in_millis = fp.getFlight().getEventTimestamp().getMillis();
            LOG.info("EventTimestamp: {}", fp.getFlight().getEventTimestamp().toString());
            String key = fp.getFlight().getField(Flight.INPUTCOLS.ORIGIN)
                    + "#" + fp.getFlight().getField(Flight.INPUTCOLS.DEST)
                    + "#" + fp.getFlight().getField(Flight.INPUTCOLS.OP_CARRIER)
                    + "#" + (Long.MAX_VALUE
                    - fp.getFlight()
                    .getFieldAsDateTime(Flight.INPUTCOLS.CRS_DEP_TIME)
                    .getMillis());

            List<Mutation> mutations = new ArrayList<>();

            for (Flight.INPUTCOLS col : Flight.INPUTCOLS.values()) {
                addCell(
                        mutations,
                        col.toString(),
                        fp.getFlight().getField(col),
                        ts_in_millis * 1000
                );
            }

            if(fp.getOntime() >= 0) {
                addCell(
                        mutations,
                        "ONTIME",
                        new DecimalFormat("0.00").format(fp.getOntime()),
                        ts_in_millis
                );
            }

            c.output(KV.of(ByteString.copyFromUtf8(key), mutations));
        }
    }
}
