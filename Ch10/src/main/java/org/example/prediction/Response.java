package org.example.prediction;

import java.util.ArrayList;
import java.util.List;

/***
 * Represents a container of the response for accessing prediction service
 */
public class Response {
    private final List<Prediction> predictions = new ArrayList<Prediction>();

    public Response(Prediction predictions) {
        this.predictions.add(predictions);
    }

    public static Response of(Prediction predictions) {
        return new Response(predictions);
    }
}
