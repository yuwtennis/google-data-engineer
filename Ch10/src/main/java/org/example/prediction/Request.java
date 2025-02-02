package org.example.prediction;

import java.util.ArrayList;
import java.util.List;

/***
 * Represents a container of the request for accessing prediction service
 */
public class Request {
    private final List<Instance> instances = new ArrayList<>();

    public Request(Instance instance) {
        this.instances.add(instance);
    }

    public static Request of(Instance instance){
        return new Request(instance);
    }

    public List<Instance> getInstances() {
        return instances;
    }
}
