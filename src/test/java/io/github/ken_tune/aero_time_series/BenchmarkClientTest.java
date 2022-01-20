package io.github.ken_tune.aero_time_series;

import io.github.ken_tune.time_series.TestConstants;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;

public class BenchmarkClientTest {
    @Test
    /**
     * Check that the randomly generated time series name is of the expected length
     */
    public void checkTimeSeriesNameGeneration(){
        TimeSeriesBenchmarkClient t = new TimeSeriesBenchmarkClient(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TimeSeriesBenchmarkClient.DEFAULT_TIME_SERIES_COUNT);
        String timeSeriesName = t.randomTimeSeriesName();
        Assert.assertTrue(timeSeriesName.length() == t.getTimeSeriesNameLength());
    }

    @Test
    /**
     * Check that time series name generation is random
     * 10,000 samples - are any identical
     */
    public void checkTimeSeriesNamesUnique(){
        TimeSeriesBenchmarkClient t = new TimeSeriesBenchmarkClient(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TimeSeriesBenchmarkClient.DEFAULT_TIME_SERIES_COUNT);

        int randomSampleCount = 10000;
        Set<String> s = new HashSet<>();
        for(int i=0;i<randomSampleCount;i++) s.add(t.randomTimeSeriesName());
        Assert.assertTrue(s.size() == randomSampleCount);
    }
}
