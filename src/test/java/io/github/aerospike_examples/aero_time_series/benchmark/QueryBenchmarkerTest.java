package io.github.aerospike_examples.aero_time_series.benchmark;

import io.github.aerospike_examples.aero_time_series.TestConstants;
import io.github.aerospike_examples.aero_time_series.TestUtilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Vector;

public class QueryBenchmarkerTest {
    /**
     Check acceleration flag presence triggers an error if found in query invocation
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
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,accelerationFactor);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
        Check timeSeriesCount flag presence triggers an error if found in query invocation
     */
    @Test
    public void timeSeriesCountFlagHandled() throws IOException {
        int timeSeriesCount = 10;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,timeSeriesCount);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
        Check intervalBetweenObservations flag presence triggers an error if found in query invocation
     */
    @Test
    public void intervalBetweenObservationsFlagHandled() throws IOException {
        int intervalBetweenObservations = 10;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,intervalBetweenObservations);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
        Check timeSeriesRange flag presence triggers an error if found in query invocation
     */
    @Test
    public void timeSeriesRangeFlagHandled() throws IOException {
        String timeSeriesRange = String.format("%d%s",1, OptionsHelper.TimeUnitIndicators.DAY);
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,timeSeriesRange);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
     * Check a good run runs without errors
     * @throws Exception Can throw exception
     */
    @Test
    public void goodRun() throws Exception{
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
