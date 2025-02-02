package org.example.prediction;
import java.util.List;

/***
 * Represents Prediction result for prediction service
 */
public class Prediction {
    List<Double> probabilities;

    public Prediction(List<Double> probabilities) {
        this.probabilities = probabilities;
    }

    public static Prediction of(List<Double> probabilities) {
        return new Prediction(probabilities);
    }

    public List<Double> getProbabilities() {
        return probabilities;
    }
}
