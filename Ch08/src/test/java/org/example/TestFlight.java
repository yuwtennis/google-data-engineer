// TODO unittest
package org.example;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFlight {

    private String testInput = null;

    @Before
    void Fixtures() {
        this.testInput = "2018-01-02,AA,19805,AA,2461,13303,1330303,32467,MIA,11278,1127805,30852,DCA,2018-01-03 01:00:00,2018-01-03 00:56:00,-4.00,37.00,2018-01-03 01:33:00,2018-01-03 03:29:00,3.00,2018-01-03 03:41:00,2018-01-03 03:32:00,-9.00,0.00,,0.00,919.00,25.79527778,-80.29000000,-18000.0,38.85138889,-77.03777778,-18000.0,arrived,2018-01-03 03:32:00";
    }
    @Test
    void AssertTrue() {
        assertTrue(true);
    }
}
