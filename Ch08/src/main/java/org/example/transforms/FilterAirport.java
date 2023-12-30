package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterAirport extends PTransform<PCollection<String>, PCollection<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(FilterAirport.class);
    private final String airportName;

    public FilterAirport(String airportName) {
        this.airportName = airportName;
    }
    public static class StringFilteringFn extends DoFn<String, String> {
        private final String filterValue;

        public StringFilteringFn(String filterValue) {
            this.filterValue = filterValue;
        }
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String input = c.element();
            if (input.contains(this.filterValue)) {
                c.output(input);
            }
        }
    }

    /**
     * Composite transform that filter out event based on keyword
     *
     * @param pCol the input PCollection of type String
     * @return the expanded PCollection of type String
     */
    public PCollection<String> expand(PCollection<String> pCol) {

        return pCol
                .apply(ParDo.of(new StringFilteringFn("MIA")))
                .apply(ParDo.of(new DoFn<String, String>(){
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info(c.element());
                        c.output(c.element());
                    }
                }));
    }
}
