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
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TimeSeriesBenchmarker.DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS,
                        TimeSeriesBenchmarker.DEFAULT_RUN_DURATION_SECONDS,TimeSeriesBenchmarker.DEFAULT_ACCELERATION_FACTOR,
                        TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT,TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT);
        TimeSeriesBenchmarkRunnable benchmarkRunnable = new TimeSeriesBenchmarkRunnable(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,1,benchmarker);
        String timeSeriesName = benchmarkRunnable.randomTimeSeriesName();
        Assert.assertTrue(timeSeriesName.length() == benchmarker.timeSeriesNameLength);
    }

    @Test
    /**
     * Check that time series name generation is random
     * 10,000 samples - are any identical
     */
    public void checkTimeSeriesNamesUnique(){
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TimeSeriesBenchmarker.DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS,
                        TimeSeriesBenchmarker.DEFAULT_RUN_DURATION_SECONDS,TimeSeriesBenchmarker.DEFAULT_ACCELERATION_FACTOR,
                        TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT,TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT);
        TimeSeriesBenchmarkRunnable benchmarkRunnable = new TimeSeriesBenchmarkRunnable(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,1,benchmarker);

        int randomSampleCount = 10000;
        Set<String> s = new HashSet<>();
        for(int i=0;i<randomSampleCount;i++) s.add(benchmarkRunnable.randomTimeSeriesName());
        Assert.assertTrue(s.size() == randomSampleCount);
    }
}
