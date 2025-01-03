package prediction;

import org.example.flight.Flight;
import org.example.flight.exceptions.EventMalformedException;
import org.example.prediction.Instance;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestInstance {

    private String event;

    @Before
    public void fixtures() throws Exception {
        this.event = "2018-01-04,WN,19393,WN,717,14771,1477104,32457,SFO,11259,1125903,30194,DAL,2018-01-04 15:00:00,2018-01-04 14:57:00,-3.00,17.00,2018-01-04 15:14:00,2018-01-04 18:05:00,4.00,2018-01-04 18:25:00,2018-01-04 18:09:00,-16.00,0.00,,0.00,1476.00,37.61888889,-122.37555556,-28800.0,32.84722222,-96.85166667,-21600.0,arrived,2018-01-04 18:09:00";
    }
    @Test
    public void testInstance() throws EventMalformedException {
        Flight f = Flight.fromCsv(this.event);
        Instance instance = Instance.of(f);

        assertTrue(true);
    }
}
