package org.example.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterAirport {
    private static final Logger LOG = LoggerFactory.getLogger(FilterAirport.class);

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

    public static class FilterAirportTransform
            extends PTransform<PCollection<String>, PCollection<String>> {

        private final String airportName;

        public FilterAirportTransform(String airportName) {
            this.airportName = airportName;
        }
        /**
         * Expands the input PCollection of strings by applying a series of transformations.
         *
         * @param pCol The input PCollection of strings.
         * @return The expanded PCollection of strings.
         */
        @Override
        public PCollection<String> expand(PCollection<String> pCol) {

            return pCol
                    .apply(ParDo.of(new StringFilteringFn(airportName)));
        }
    }
}
