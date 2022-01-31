package io.github.ken_tune.aero_time_series;

import io.github.ken_tune.aero_time_series.Constants;
import io.github.ken_tune.aero_time_series.DataPoint;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class DataPointTest {
    @Test
    // Test the DataPoint equals function
    public void testEqualsFunction(){
        long t1 = 1;
        long t2 = 2;
        double v1 = 1;
        double v2 = 2;

        DataPoint d = new DataPoint(t1,v1);

        // Are points equal to themselves
        Assert.assertTrue(d.equals(d));
        // Are they equal to points where timestamp / value are the same
        Assert.assertTrue(d.equals(new DataPoint(t1,v1)));
        // Are they equal to points where timestamp / value are not the same
        Assert.assertFalse(d.equals(new DataPoint(t1,v2)));
        Assert.assertFalse(d.equals(new DataPoint(t2,v1)));
        Assert.assertFalse(d.equals(new DataPoint(t2,v2)));

    }
}
