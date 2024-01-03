package org.example;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.example.transforms.ParDoWithSideInput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;


public class TestParDoWithSideInput {
    @Rule
    public final transient TestPipeline tp = TestPipeline.create();
    private List<String> trainingCsv;

    @Before
    public void Fixtures() {
        this.trainingCsv = Collections.singletonList("2018-01-02,True");
    }
    @Test
    public void TestParseTrainingDayCsv() {
        PCollection<KV<String, String>> output = tp
                .apply(Create.of(this.trainingCsv))
                .apply(ParDo.of(new ParDoWithSideInput.ParseTrainingDayCsv()));

        PAssert.that(output).containsInAnyOrder(KV.of("2018-01-02", ""));
        tp.run();
    }
}
