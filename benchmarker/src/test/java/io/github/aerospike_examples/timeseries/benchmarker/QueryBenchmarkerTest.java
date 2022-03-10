package io.github.aerospike_examples.timeseries.benchmarker;

import io.github.aerospike_examples.timeseries.benchmarker.util.TestConstants;
import io.github.aerospike_examples.timeseries.benchmarker.util.TestUtilities;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryBenchmarkerTest {

    @SuppressWarnings({"FieldCanBeLocal", "CanBeFinal"}) // Used during development
    private static boolean doTeardown = true;

    /**
     * Check acceleration flag presence triggers an error if found in query invocation
     */
    @Test
    public void accelerationFlagHandled() throws IOException {
        int accelerationFactor = 5;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, OptionsHelper.BenchmarkModes.QUERY,
                        runDuration, accelerationFactor);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
     * Check timeSeriesCount flag presence triggers an error if found in query invocation
     */
    @Test
    public void timeSeriesCountFlagHandled() throws IOException {
        int timeSeriesCount = 10;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, OptionsHelper.BenchmarkModes.QUERY,
                        runDuration, timeSeriesCount);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
     * Check intervalBetweenObservations flag presence triggers an error if found in query invocation
     */
    @Test
    public void intervalBetweenObservationsFlagHandled() throws IOException {
        int intervalBetweenObservations = 10;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, OptionsHelper.BenchmarkModes.QUERY,
                        runDuration, intervalBetweenObservations);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
     * Check timeSeriesRange flag presence triggers an error if found in query invocation
     */
    @Test
    public void timeSeriesRangeFlagHandled() throws IOException {
        String timeSeriesRange = String.format("%d%s", 1, OptionsHelper.TimeUnitIndicators.DAY);
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, OptionsHelper.BenchmarkModes.QUERY,
                        runDuration, timeSeriesRange);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
     * Check a good query run
     * Do this by matching the messages returned vs a regex
     *
     * @throws Exception Can throw exception
     */
    @Test
    public void goodQueryRun() throws Exception {
        // First set some data up to query - 10 days worth
        int intervalBetweenUpdates = 10;
        int threadCount = 10;
        int timeSeriesCount = 10;
        int recordsPerBlock = 1000;
        long timeSeriesRangeSeconds = 86400 * 10;

        TimeSeriesBenchmarker insertBenchmarker = TestUtilities.batchInsertBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,
                TestConstants.TIME_SERIES_TEST_SET, intervalBetweenUpdates, timeSeriesRangeSeconds,
                threadCount, timeSeriesCount, recordsPerBlock, TestConstants.RANDOM_SEED);

        insertBenchmarker.run();

        // Now run the query benchmarker
        int queryRunDurationSeconds = 10;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                        OptionsHelper.BenchmarkModes.QUERY, threadCount, queryRunDurationSeconds);

        // Run the query benchmarker and capture the console output
        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);
        for (String output : consoleOutput) System.out.println(output);

        // Check we get the expected number of status messages
        Pattern pattern = Pattern.compile(
                "Run time : \\d+ sec, Query count : \\d+, Current queries/sec \\d+.\\d{3}, Current latency \\d+.\\d{3}s, Avg latency \\d+.\\d{3}s, Cumulative queries/sec \\d+.\\d{3}",
                Pattern.CASE_INSENSITIVE);

        int runTimeMessageCount = 0;
        for (String aConsoleOutput : consoleOutput) {
            Matcher matcher = pattern.matcher(aConsoleOutput);
            if (matcher.find()) runTimeMessageCount++;
        }

        Assert.assertTrue(runTimeMessageCount >= queryRunDurationSeconds);
    }

    @After
    // Truncate the time series set
    public void teardown() {
        if (doTeardown) {
            TestUtilities.removeTimeSeriesTestDataForSet(TestConstants.TIME_SERIES_TEST_SET);
        }
    }

}
