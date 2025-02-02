package org.example.prediction;

import com.google.cloud.aiplatform.v1.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.flight.Flight;
import org.example.flight.FlightPred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


public class PredictionService {

    private static final String SERVICE_ENDPOINT = "aiplatform.googleapis.com:443";
    private static final Logger LOG = LoggerFactory.getLogger(PredictionService.class);

    public static class Predict extends PTransform<PCollection<Flight>, PCollection<FlightPred>> {

        final String projectId;
        final String locationName;
        final String endpointId;

        public Predict(String projectId, String locationName, String endpointId) {
            this.projectId = projectId;
            this.locationName = locationName;
            this.endpointId = endpointId;
        }

        @Override
        public PCollection<FlightPred> expand(PCollection<Flight> input)  {
            // TODO Implement batch predict
            return input.apply(ParDo.of(new DoFn<Flight, FlightPred>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception{
                    Flight f = c.element();
                    Instance instance = Instance.of(f);
                    Collection<Instance> instances = Arrays.asList(instance);
                    Gson gson = new GsonBuilder().create();
                    String json = gson.toJson(instances);

                    PredictionServiceSettings predictionServiceSettings = null;
                    predictionServiceSettings = PredictionServiceSettings
                                .newBuilder()
                                .setEndpoint(locationName + "-" + SERVICE_ENDPOINT)
                                .build();

                    if(predictionServiceSettings != null) {
                        try (PredictionServiceClient client =
                                     PredictionServiceClient.create(predictionServiceSettings)) {
                            EndpointName endpoint = EndpointName.of(
                                    projectId,
                                    locationName,
                                    endpointId);
                            ListValue.Builder listValue = ListValue.newBuilder();
                            JsonFormat.parser().merge(json, listValue);
                            List<Value> instanceList = listValue.getValuesList();
                            List<Double> predictedResult = new ArrayList<>();
                            PredictRequest predictRequest = PredictRequest
                                    .newBuilder()
                                    .setEndpoint(endpoint.toString())
                                    .addAllInstances(instanceList)
                                    .build();
                            PredictResponse predictResponse = client.predict(predictRequest);
                            for (Value v : predictResponse.getPredictionsList()) {
                                predictedResult.add(v.getListValue().getValues(0).getNumberValue());
                            }

                            FlightPred flightPred = FlightPred.of(
                                    f,
                                    Prediction.of(predictedResult));
                            c.output(flightPred);
                        } catch (Exception e) {
                            LOG.warn("Failed with prediction: {}", Arrays.toString(f.getFields()));
                            LOG.warn(e.getMessage());
                        }
                    }
                }
            }));
        }
    }
}
