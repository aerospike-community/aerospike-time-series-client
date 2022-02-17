package io.github.aerospike_examples.aero_time_series.benchmark;

import io.github.aerospike_examples.aero_time_series.TestConstants;
import io.github.aerospike_examples.aero_time_series.TestUtilities;
import org.junit.Test;

public class QueryBenchmarkTest {
    @Test
    public void scratch(){
        TestUtilities.removeTimeSeriesTestDataForSet(TestConstants.TIME_SERIES_TEST_SET);

        int intervalBetweenUpdates = 10;
        int threadCount = 10;
        int timeSeriesCount = 10;
        int recordsPerBlock = 1000;
        long timeSeriesRangeSeconds = 86400 * 10;
        int queryRunDurationSeconds = 10;

        TimeSeriesBenchmarker benchmarker = TestUtilities.batchInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                TestConstants.TIME_SERIES_TEST_SET,intervalBetweenUpdates,timeSeriesRangeSeconds,
                threadCount,timeSeriesCount,recordsPerBlock, TestConstants.RANDOM_SEED);

        benchmarker.run();

        TimeSeriesBenchmarker readBenchmarker = TestUtilities.queryBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,
                queryRunDurationSeconds,threadCount,TestConstants.RANDOM_SEED);
        readBenchmarker.run();

    }

}
