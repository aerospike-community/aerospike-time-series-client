package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import io.github.ken_tune.time_series.Constants;
import io.github.ken_tune.time_series.TestConstants;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;

public class BenchmarkerTest {
    private boolean doTeardown = false;

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

    @Test
    /**
     * Basic test to make sure that the benchmarker runs for the expected amount of time and delivers expected updates
     */
    public void checkVanillaBenchmarkDurationAndUpdates(){
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 1;
        int threadCount = 1;
        int timeSeriesCount = 1;
        // Set test tolerance to 10% as we are only expecting 10 updates and could get 9 or 11
        int testTolerancePct = 10;
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,runDurationSeconds,accelerationFactor,
                        threadCount,timeSeriesCount);
        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(valueInTolerance(runDurationSeconds * 1000,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
    }

    /**
     * Check that the acceleration factor is observed
     * This can be seen via the number of updates
     */
    @Test
    public void checkAccelerationFactorObserved(){
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 1;
        int timeSeriesCount = 1;
        int testTolerancePct = 5;
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,runDurationSeconds,accelerationFactor,
                        threadCount,timeSeriesCount);
        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(valueInTolerance(runDurationSeconds * 1000,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
    }

    /**
     * Check that thread count can be set to value other than 1 and still get correct results
     * This can be seen via the number of updates
     */
    @Test
    public void checkTimeSeriesFactorObserved(){
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 5;
        int timeSeriesCount = 100;
        int testTolerancePct = 5;
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,runDurationSeconds,accelerationFactor,
                        threadCount,timeSeriesCount);
        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(valueInTolerance(runDurationSeconds * 1000,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
    }

    /**
     * Check that the time series count factor is observed
     * This can be seen via the number of updates
     */
    @Test
    public void checkThreadCountObserved(){
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 1;
        int timeSeriesCount = 10;
        int testTolerancePct = 5;
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,runDurationSeconds,accelerationFactor,
                        threadCount,timeSeriesCount);
        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(valueInTolerance(runDurationSeconds * 1000,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
    }
    /**
     * Check that the interval between updates is observed
     * This can be seen via the number of updates
     */
    @Test
    public void checkUpdateIntervalObserved(){
        int intervalBetweenUpdates = 2;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 1;
        int timeSeriesCount = 10;
        int testTolerancePct = 5;
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,runDurationSeconds,accelerationFactor,
                        threadCount,timeSeriesCount);
        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(valueInTolerance(runDurationSeconds * 1000,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount / intervalBetweenUpdates,
                benchmarker.totalUpdateCount(),testTolerancePct));
    }

    @After
    // Truncate the time series set
    public void teardown(){
        AerospikeClient asClient = new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT);
        if(doTeardown) {
            asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, Constants.AS_TIME_SERIES_SET, null);
            asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, Constants.AS_TIME_SERIES_INDEX_SET, null);
        }
    }

    private static boolean valueInTolerance(double expectedValue, double actualValue,double tolerancePct){
        return Math.abs((expectedValue - actualValue)/actualValue) < tolerancePct / 100;
    }
}
