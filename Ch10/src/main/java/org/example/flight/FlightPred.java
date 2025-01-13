package org.example.flight;

import com.google.api.services.bigquery.model.TableRow;
import com.google.bigtable.v2.Mutation;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.example.prediction.Prediction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;

import static org.example.flight.models.BigQuery.FLIGHTBQTYPES;

@DefaultCoder(AvroCoder.class)
public class FlightPred {
    private static final Logger LOG = LoggerFactory.getLogger(FlightPred.class);

    static final int ONTIME_DEFAULT = -5;
    private Flight flight;
    private double ontime;

    /***
     *
     * @param f
     * @param p
     * @return
     */
    public static FlightPred of(Flight f, Prediction p) {
        FlightPred fp = new FlightPred();
        LOG.info("Predicted result: {}", p.getProbabilities());
        fp.flight = f;
        fp.ontime = p.getProbabilities().isEmpty() ?
                ONTIME_DEFAULT : decideOntime(f, p.getProbabilities().get(0));
        LOG.info("Ontime: {}", fp.ontime);
        return fp;
    }

    public String toCsv() {
        String csv = String.join(",", flight.getFields());
        if (ontime >= 0) {
            csv += "," + new DecimalFormat("0.00").format(ontime);
        } else {
            csv = csv + ","; // Empty string
        }
        return csv;
    }

    /***
     *
     * @param f
     * @param probability
     * @return
     */
    private static double decideOntime(Flight f, double probability) {
        double ontime = ONTIME_DEFAULT;

        if (f.isNotCancelled() && f.isNotDiverted()) {
            if (f.isArrivedEvent()) {
                // After arrival
                ontime = f.getFieldAsFloat(Flight.INPUTCOLS.ARR_DELAY) < 15 ? 1 : 0;
            } else {
                // At wheelsoff
                ontime = probability;
            }
        } else {
            // At departure
            ontime = probability;
        }

        return ontime;
    }

    public Flight getFlight() {
        return flight;
    }

    public double getOntime() {
        return ontime;
    }
}
