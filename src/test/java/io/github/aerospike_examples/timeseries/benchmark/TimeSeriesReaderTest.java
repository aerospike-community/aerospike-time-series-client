package io.github.aerospike_examples.timeseries.benchmark;

import io.github.aerospike_examples.timeseries.TestConstants;
import io.github.aerospike_examples.timeseries.TestUtilities;
import io.github.aerospike_examples.timeseries.benchmarker.OptionsHelper;
import io.github.aerospike_examples.timeseries.benchmarker.TimeSeriesReader;
import org.junit.Test;

public class TimeSeriesReaderTest {
    @Test
    public void goodReaderRun() throws Exception {

        // First set some data up to query - 10 days worth
        int intervalBetweenUpdates = 10;
        int threadCount = 10;
        int timeSeriesCount = 10;
        long timeSeriesRangeSeconds = 6 * 10 * intervalBetweenUpdates;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                        OptionsHelper.BenchmarkModes.BATCH_INSERT, intervalBetweenUpdates, threadCount, timeSeriesCount,
                        timeSeriesRangeSeconds);

        TestUtilities.runBenchmarkerGetOutput(commandLineArguments);
        // Now run the time series reader

        // Create the string argument array
        String readerFormatString = String.format("-%s %%s -%s %%s -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG);

        String readerFormatCommandLineArguments =
                String.format(readerFormatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET);

        TimeSeriesReader.main(readerFormatCommandLineArguments.split(" "));
    }
}
