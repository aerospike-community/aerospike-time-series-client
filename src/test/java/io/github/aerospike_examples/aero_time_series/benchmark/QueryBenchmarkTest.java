package io.github.aerospike_examples.aero_time_series.benchmark;

import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.ScanPolicy;
import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.TestConstants;
import io.github.aerospike_examples.aero_time_series.TestUtilities;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesInfo;
import org.junit.Test;

import java.util.Date;
import java.util.Vector;

public class QueryBenchmarkTest {
    @Test
    public void scratch() throws Exception{
        TestUtilities.removeTimeSeriesTestDataForSet(TestConstants.TIME_SERIES_TEST_SET);

        int intervalBetweenUpdates = 10;
        int threadCount = 10;
        int timeSeriesCount = 10;
        int recordsPerBlock = 1000;
        long timeSeriesRangeSeconds = 86400 * 10;
        int queryRunDuratinoSeconds = 10;

        // Keep track of the start time - useful when we retrieve the data points
        long startTime = System.currentTimeMillis();

        TimeSeriesBenchmarker benchmarker = TestUtilities.batchInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                TestConstants.TIME_SERIES_TEST_SET,intervalBetweenUpdates,timeSeriesRangeSeconds,
                threadCount,timeSeriesCount,recordsPerBlock, TestConstants.RANDOM_SEED);

        benchmarker.run();

        TimeSeriesBenchmarker readBenchmarker = TestUtilities.queryBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,
                queryRunDuratinoSeconds,threadCount,TestConstants.RANDOM_SEED);
        readBenchmarker.run();

    }

}
