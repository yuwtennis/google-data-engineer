package org.example.flight.delays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.flight.Flight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class AvgDepartureDelayReferer {
    private final Pipeline pipeline;
    private static final Logger LOG = LoggerFactory.getLogger(AvgDepartureDelayReferer.class);

    public AvgDepartureDelayReferer(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    private static class CsvParser extends DoFn<String, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] departure = c.element().split(",");
            c.output(KV.of(departure[0], Double.parseDouble(departure[1])));
        }
    }

    /**
     *
     * @param path
     * @return
     */
    public PCollectionView<Map<String, Double>> asView(String path) {
        return pipeline.apply(
                "", TextIO.read().from(path))
                .apply("Parse", ParDo.of(new CsvParser()))
                .apply("toView", View.asMap());
    }

    /**
     *
     * @param flights
     * @param depDelayView
     * @return
     */
    public static PCollection<KV<String, Flight>> lookupOriginDepDelay(
            PCollection<Flight> flights,
            PCollectionView<Map<String, Double>> depDelayView) {
        return flights.apply("CalcDepDelay", ParDo.of(new DoFn<Flight, KV<String, Flight>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Flight f = c.element().newCopy();
                String origin = f.getField(Flight.INPUTCOLS.ORIGIN) + ":" + f.getDepartureHour();
                Double depDelay = c.sideInput(depDelayView).get(origin);
                double originDelay = (depDelay == null) ? 0 : depDelay;

                f.setAvgDepartureDelay((float) originDelay);
                c.output(KV.of(f.getField(Flight.INPUTCOLS.DEST), f));
            }
        }).withSideInputs(depDelayView));
    }
}
