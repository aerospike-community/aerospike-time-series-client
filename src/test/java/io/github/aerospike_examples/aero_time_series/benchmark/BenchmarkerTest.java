package io.github.aerospike_examples.aero_time_series.benchmark;

import com.aerospike.client.*;
import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.TestConstants;
import io.github.aerospike_examples.aero_time_series.TestUtilities;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.client.DataPoint;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;
import org.junit.*;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BenchmarkerTest {
    @SuppressWarnings({"FieldCanBeLocal", "CanBeFinal"}) // Used during development
    private static boolean doTeardown = true;

    /**
     * Check that the randomly generated time series name is of the expected length
     */
    @Test
    public void checkTimeSeriesNameGeneration() {
        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                TimeSeriesBenchmarker.DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS, TimeSeriesBenchmarker.DEFAULT_RUN_DURATION_SECONDS,
                TimeSeriesBenchmarker.DEFAULT_ACCELERATION_FACTOR, TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT,
                TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT);

        RealTimeInsertTimeSeriesRunnable benchmarkRunnable = new RealTimeInsertTimeSeriesRunnable(new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, 1, benchmarker);
        String timeSeriesName = benchmarkRunnable.randomTimeSeriesName();
        Assert.assertEquals(timeSeriesName.length(), benchmarker.timeSeriesNameLength);
    }

    /**
     * Check that time series name generation is random
     * 10,000 samples - are any identical
     */
    @Test
    public void checkTimeSeriesNamesUnique() {
        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                TimeSeriesBenchmarker.DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS, TimeSeriesBenchmarker.DEFAULT_RUN_DURATION_SECONDS,
                TimeSeriesBenchmarker.DEFAULT_ACCELERATION_FACTOR, TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT,
                TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT);

        RealTimeInsertTimeSeriesRunnable benchmarkRunnable = new RealTimeInsertTimeSeriesRunnable(new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, 1, benchmarker);

        int randomSampleCount = 10000;
        Set<String> s = new HashSet<>();
        for (int i = 0; i < randomSampleCount; i++) s.add(benchmarkRunnable.randomTimeSeriesName());
        Assert.assertEquals(s.size(), randomSampleCount);
    }

    /**
     * Basic test to make sure that the benchmarker runs for the expected amount of time and delivers expected updates
     */
    @Test
    public void checkVanillaBenchmarkDurationAndUpdates() {
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 1;
        int threadCount = 1;
        int timeSeriesCount = 1;
        // Set test tolerance to 20% as we are using small numbers - variability could be slightly higher than 10% at any given time
        int testTolerancePct = 20;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                intervalBetweenUpdates, runDurationSeconds, accelerationFactor, threadCount, timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * Constants.MILLISECONDS_IN_SECOND, benchmarker.averageThreadRunTimeMs(), testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount, benchmarker.totalUpdateCount(), testTolerancePct));
    }

    /**
     * Check that the acceleration factor is observed
     * This can be seen via the number of updates
     */
    @Test
    public void checkAccelerationFactorObserved() {
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 1;
        int timeSeriesCount = 1;
        int testTolerancePct = 5;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                intervalBetweenUpdates, runDurationSeconds, accelerationFactor, threadCount, timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * Constants.MILLISECONDS_IN_SECOND, benchmarker.averageThreadRunTimeMs(), testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount, benchmarker.totalUpdateCount(), testTolerancePct));
    }

    /**
     * Check that thread count can be set to value other than 1 and still get correct results
     * This can be seen via the number of updates
     */
    @Test
    public void checkTimeSeriesCountObserved() {
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 5;
        int timeSeriesCount = 100;
        int testTolerancePct = 5;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, intervalBetweenUpdates, runDurationSeconds, accelerationFactor, threadCount, timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * Constants.MILLISECONDS_IN_SECOND, benchmarker.averageThreadRunTimeMs(), testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount, benchmarker.totalUpdateCount(), testTolerancePct));
    }

    /**
     * Check that the time series count factor is observed
     * This can be seen via the number of updates
     */
    @Test
    public void checkThreadCountObserved() {
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 1;
        int timeSeriesCount = 10;
        int testTolerancePct = 5;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                intervalBetweenUpdates, runDurationSeconds, accelerationFactor, threadCount, timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * Constants.MILLISECONDS_IN_SECOND, benchmarker.averageThreadRunTimeMs(), testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount, benchmarker.totalUpdateCount(), testTolerancePct));
    }

    /**
     * Check that the interval between updates is observed
     * This can be seen via the number of updates
     */
    @Test
    public void checkUpdateIntervalObserved() {
        int intervalBetweenUpdates = 2;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 1;
        int timeSeriesCount = 10;
        int testTolerancePct = 5;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                intervalBetweenUpdates, runDurationSeconds, accelerationFactor, threadCount, timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * Constants.MILLISECONDS_IN_SECOND, benchmarker.averageThreadRunTimeMs(), testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount / intervalBetweenUpdates,
                benchmarker.totalUpdateCount(), testTolerancePct));
    }

    /**
     * If we exceed the number of key updates we can accommodate per second, is there a warning message?
     */
    @Test
    public void checkHotKeyCheck() throws Exception {
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 101;
        int threadCount = 1;
        int timeSeriesCount = 1;
        double updatesPerSecond = (double) accelerationFactor * timeSeriesCount / intervalBetweenUpdates;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, intervalBetweenUpdates, runDurationSeconds, accelerationFactor, threadCount, timeSeriesCount);

        Vector<String> consoleOutput = runBenchmarkerGetOutput(benchmarker);

        Assert.assertEquals(consoleOutput.get(5), String.format("!!! Single key updates per second rate %.3f exceeds max recommended rate %d", updatesPerSecond, Constants.SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC));
    }

    /**
     * Check drift is simulated correctly
     */
    @Test
    public void checkDriftSimulatedCorrectly() {
        // Generate 500 observations via acceleration
        int intervalBetweenUpdates = 60;
        int runDurationSeconds = 10;
        int accelerationFactor = intervalBetweenUpdates * 50;
        int threadCount = 1;
        int timeSeriesCount = 1;
        // Set volatility to zero
        int dailyDriftPct = 10;
        int dailyVolatilityPct = 0;
        // Check the simulation generates drift correctly when volatility is zero
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,
                        intervalBetweenUpdates, runDurationSeconds, accelerationFactor,
                        threadCount, timeSeriesCount, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK,
                        0, dailyDriftPct, dailyVolatilityPct, TestConstants.RANDOM_SEED);
        long startTime = System.currentTimeMillis();
        benchmarker.run();
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);

        DataPoint[] dataPoints = timeSeriesClient.getPoints(TestConstants.REFERENCE_TIME_SERIES_NAME, new Date(startTime), new Date(startTime + (long) accelerationFactor * runDurationSeconds * 1000));
        double[] values = new double[dataPoints.length];
        for (int i = 0; i < values.length; i++) values[i] = dataPoints[i].getValue();
        TimeSeriesSimulatorTest.checkDailyDriftPct(values, dailyDriftPct, intervalBetweenUpdates, 10);
    }

    /**
     * Check volatility is simulated correctly
     */
    @Test
    public void checkVolatilitySimulatedCorrectly() {
        teardown();
        doTeardown = false;
        // Generate 500 observations via acceleration
        int intervalBetweenUpdates = 60;
        int runDurationSeconds = 10;
        int accelerationFactor = intervalBetweenUpdates * 50;
        int threadCount = 1;
        int timeSeriesCount = 1;
        // Set drift to zero so we can focus on volatility
        int dailyDriftPct = 0;
        int dailyVolatilityPct = 10;
        // Check the simulation generates drift correctly when volatility is zero
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,
                        intervalBetweenUpdates, runDurationSeconds, accelerationFactor,
                        threadCount, timeSeriesCount, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK, 0, dailyDriftPct, dailyVolatilityPct, TestConstants.RANDOM_SEED);
        long startTime = System.currentTimeMillis();
        benchmarker.run();
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);

        DataPoint[] dataPoints = timeSeriesClient.getPoints(TestConstants.REFERENCE_TIME_SERIES_NAME, new Date(startTime), new Date(startTime + (long) accelerationFactor * runDurationSeconds * 1000));
        double[] values = new double[dataPoints.length];
        for (int i = 0; i < values.length; i++) values[i] = dataPoints[i].getValue();
        TimeSeriesSimulatorTest.checkDailyVolatilityPct(values, dailyVolatilityPct, intervalBetweenUpdates, 10);
    }

    /**
      Check that we get a warning message if the simulation is underflowing the expected rate
     */
    @Test
    public void checkUnderflowCheck() throws Exception{
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 100;
        int threadCount = 1;
        int timeSeriesCount = 100;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,
                intervalBetweenUpdates,runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        Vector<String> consoleOutput = runBenchmarkerGetOutput(benchmarker);
        int warningCount = 0;
        for (String aConsoleOutput : consoleOutput) {
            System.out.println(aConsoleOutput);
            if (aConsoleOutput.startsWith(
                    String.format("!!!Update rate should be %.3f, actually", benchmarker.expectedUpdatesPerSecond())) && aConsoleOutput.endsWith(" - underflow"))
                warningCount++;
        }
        // We should get a warning on every second, for rounding reasons the first two may not happen
        Assert.assertTrue(warningCount >= runDurationSeconds -2);
    }


    /**
      Check output is as expected for a test case
      Should see updates per second / updates per second per time series header
      Should see a status message every second plus an initial status message
     */
    @Test
    public void correctMainOutput() throws IOException, Utilities.ParseException{
        int intervalBetweenUpdates = 2;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 1;
        int timeSeriesCount = 10;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG,OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,
                        intervalBetweenUpdates, runDurationSeconds,accelerationFactor,threadCount,timeSeriesCount);

        // Initialise the benchmarker using the String[]
        TimeSeriesBenchmarker benchmarker = TimeSeriesBenchmarker.initBenchmarkerFromStringArgs(commandLineArguments.split(" "));
        // Capture the console output
        Vector<String> consoleOutput = runBenchmarkerGetOutput(benchmarker);
        for(String output: consoleOutput) System.out.println(output);
        // Check the two header messages
        Assert.assertEquals(consoleOutput.get(2), String.format("Updates per second : %.3f", (double) accelerationFactor * timeSeriesCount / intervalBetweenUpdates));
        Assert.assertEquals(consoleOutput.get(3), String.format("Updates per second per time series : %.3f", (double) accelerationFactor * timeSeriesCount / intervalBetweenUpdates / timeSeriesCount));

        // Check we get the expected number of status messages
        Pattern pattern = Pattern.compile(
                "Run time : \\d+ seconds, Update count : \\d+, Current updates per second : \\d+.\\d{3}, Cumulative updates per second : \\d+.\\d{3}",
                Pattern.CASE_INSENSITIVE);

        int runTimeMessageCount = 0;
        for (String aConsoleOutput : consoleOutput) {
            Matcher matcher = pattern.matcher(aConsoleOutput);
            if (matcher.find()) runTimeMessageCount++;
        }

        Assert.assertTrue(runTimeMessageCount >= runDurationSeconds);
    }

    /**
      Check that when running in batch mode
      1) The expected number of data points are created
      2) The number of 'blocks' is as expected
      3) The drift and variance of the resulting series is as expected

      This is a long running test - it needs to be to get the required convergence on drift and volatility

      We are also running for multiple time series to check that works successfully
     */
    @Test
    public void batchModeCheck(){
        int intervalBetweenUpdates = 10;
        int threadCount = 10;
        int timeSeriesCount = 10;
        int recordsPerBlock = 1000;
        long timeSeriesRangeSeconds = 86400 * 100;

        // Keep track of the start time - useful when we retrieve the data points
        long startTime = System.currentTimeMillis();

        TimeSeriesBenchmarker benchmarker = TestUtilities.batchInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                TestConstants.TIME_SERIES_TEST_SET,intervalBetweenUpdates,timeSeriesRangeSeconds,
                threadCount,timeSeriesCount,recordsPerBlock, TestConstants.RANDOM_SEED);

        benchmarker.run();

        Vector<String> timeSeriesNames = Utilities.getTimeSeriesNames(TestUtilities.defaultTimeSeriesClient());
        for (String timeSeriesName : timeSeriesNames) {
            System.out.println(String.format("Checking time series with name %s", timeSeriesName));
            TimeSeriesClient timeSeriesClient = new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT),
                    TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,
                    Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);

            // Widen the range slightly to allow for the fact that the benchmarker introduces variability in the sample times of OBSERVATION_INTERVAL_VARIABILITY_PCT
            DataPoint[] dataPoints = timeSeriesClient.getPoints(timeSeriesName, new Date(startTime - Constants.MILLISECONDS_IN_SECOND),
                    new Date(startTime + (1 + timeSeriesRangeSeconds) * Constants.MILLISECONDS_IN_SECOND));

            Assert.assertTrue(Utilities.valueInTolerance(timeSeriesRangeSeconds / intervalBetweenUpdates, dataPoints.length, 5));

            // Check that the number of blocks stored is as expected
            int timeSeriesBlocks = TestUtilities.blockCountForTimeseries(timeSeriesClient, TestConstants.REFERENCE_TIME_SERIES_NAME);
            int expectedTimeSeriesBlocks = (int) Math.ceil((double) timeSeriesRangeSeconds / (recordsPerBlock * intervalBetweenUpdates));
            Assert.assertEquals(timeSeriesBlocks, expectedTimeSeriesBlocks);

            // Check that the drift and variance are as expected
            double[] values = new double[dataPoints.length];
            for (int j = 0; j < values.length; j++) values[j] = dataPoints[j].getValue();

            TimeSeriesSimulatorTest.checkDailyDriftPct(values, TimeSeriesBenchmarker.DEFAULT_DAILY_DRIFT_PCT, intervalBetweenUpdates, 30);
            TimeSeriesSimulatorTest.checkDailyVolatilityPct(values, TimeSeriesBenchmarker.DEFAULT_DAILY_VOLATILITY_PCT, intervalBetweenUpdates, 10);
        }
    }

    /**
      Check parse exception is well handled
     */
    @Test
    public void cmdLineParseExceptionHandled() throws IOException {
        int intervalBetweenUpdates = 2;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        String badThreadCount = "x";
        int timeSeriesCount = 10;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG,
                OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,
                        intervalBetweenUpdates, runDurationSeconds,accelerationFactor,badThreadCount,timeSeriesCount);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag should have an integer argument. Argument supplied is %s", OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, badThreadCount));
    }

    /**
      Check time series range flag presence triggers an error if found in real time insert invocation
     */
    @Test
    public void timeSeriesRangeFlagHandled() throws IOException{
        int intervalBetweenUpdates = 2;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 5;
        int timeSeriesCount = 10;
        int timeSeriesRange = 500;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG,
                OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,
                        intervalBetweenUpdates, runDurationSeconds,accelerationFactor,threadCount,timeSeriesCount,timeSeriesRange);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);
        for(String output: consoleOutput) System.out.println(output);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.REAL_TIME_INSERT));
    }

    /**
      Check run duration flag presence triggers an error if found in batch insert invocation
     */
    @Test
    public void runDurationFlagHandled() throws IOException{
        int intervalBetweenUpdates = 2;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 5;
        int timeSeriesCount = 10;
        int timeSeriesRange = 500;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG,
                OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,OptionsHelper.BenchmarkModes.BATCH_INSERT,
                        intervalBetweenUpdates, runDurationSeconds,accelerationFactor,threadCount,timeSeriesCount,timeSeriesRange);

        TimeSeriesBenchmarker.main(commandLineArguments.split(" "));
        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.BATCH_INSERT));
    }

    /**
      Check acceleration flag presence triggers an error if found in batch insert invocation
     */
    @Test
    public void accelerationFlagHandled() throws IOException{
        int intervalBetweenUpdates = 2;
        int accelerationFactor = 5;
        int threadCount = 5;
        int timeSeriesCount = 10;
        int timeSeriesRange = 500;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,OptionsHelper.BenchmarkModes.BATCH_INSERT,
                        intervalBetweenUpdates, accelerationFactor,threadCount,timeSeriesCount,timeSeriesRange);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.BATCH_INSERT));
    }

    /**
      Check bad run modes are handled well
     */
    @Test
    public void badModeHandling() throws IOException {
        int intervalBetweenUpdates = 2;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 5;
        int timeSeriesCount = 10;
        String badMode = "badMode";

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG,
                OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,badMode,
                        intervalBetweenUpdates, runDurationSeconds,accelerationFactor,threadCount,timeSeriesCount);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("%s is an invalid run mode. Please use %s, %s or %s", badMode,
                OptionsHelper.BenchmarkModes.REAL_TIME_INSERT, OptionsHelper.BenchmarkModes.BATCH_INSERT,OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
      Check time series range is correctly converted for each possible suffix option
     */
    @Test
    public void timeSeriesRangeConversionCheck() throws Utilities.ParseException{
        timeSeriesRangeParsingCheck(20,OptionsHelper.TimeUnitIndicators.SECOND,1);
        timeSeriesRangeParsingCheck(45,OptionsHelper.TimeUnitIndicators.MINUTE,60);
        timeSeriesRangeParsingCheck(12,OptionsHelper.TimeUnitIndicators.HOUR,60*60);
        timeSeriesRangeParsingCheck(5,OptionsHelper.TimeUnitIndicators.DAY,24*60*60);
        timeSeriesRangeParsingCheck(3,OptionsHelper.TimeUnitIndicators.YEAR,365*24*60*60);
        timeSeriesRangeParsingCheck(20,"",1);

    }

    /**
      Check time series range format value matches number followed by one of Y,D,H,M,S or no unit
      and that we throw an error if not
     */
    @Test
    public void badTimeSeriesRange() throws IOException {
        badTimeSeriesRangeStringCheck("50X");
        badTimeSeriesRangeStringCheck("X50");
    }

    /**
      Check that if we use a different set name then everything still works
     */
    @Test
    public void setFlagCheck() {
        int intervalBetweenUpdates = 60;
        int threadCount = 5;
        int timeSeriesCount = 1;
        int timeSeriesRangeSeconds = 24 * 60 * 60;
        String alternativeTimeSeriesSet = "TimeSeriesSet2";

        // Keep track of the start time - useful when we retrieve the data points
        long startTime = System.currentTimeMillis();

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE, alternativeTimeSeriesSet,
                        OptionsHelper.BenchmarkModes.BATCH_INSERT, intervalBetweenUpdates, threadCount, timeSeriesCount,
                        timeSeriesRangeSeconds);

        TimeSeriesBenchmarker.main(commandLineArguments.split(" "));

        Vector<String> timeSeriesNames = Utilities.getTimeSeriesNames(TestUtilities.defaultTimeSeriesClient());
        for (String timeSeriesName : timeSeriesNames) {
            System.out.println(String.format("Checking time series with name %s", timeSeriesName));
            TimeSeriesClient timeSeriesClient = new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT),
                    TestConstants.AEROSPIKE_NAMESPACE, alternativeTimeSeriesSet,
                    Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);

            // Widen the range slightly to allow for the fact that the benchmarker introduces variability in the sample times of OBSERVATION_INTERVAL_VARIABILITY_PCT
            DataPoint[] dataPoints = timeSeriesClient.getPoints(timeSeriesName, new Date(startTime - Constants.MILLISECONDS_IN_SECOND),
                    new Date(startTime + (1 + timeSeriesRangeSeconds) * Constants.MILLISECONDS_IN_SECOND));

            System.out.println(String.format(
                    "Time series range %d Interval between updates %d, datapoints.length %d",timeSeriesRangeSeconds,intervalBetweenUpdates,dataPoints.length));
            Assert.assertTrue(Utilities.valueInTolerance(timeSeriesRangeSeconds / intervalBetweenUpdates, dataPoints.length, 5));
        }
        TestUtilities.removeTimeSeriesTestDataForSet(alternativeTimeSeriesSet);
    }

    /**
     * Private function that checks if a given 'bad' time series range string gets the expected error message
     * when calling the Benchmarker
     * @param badTimeSeriesRangeString the bad string
     */
    private void badTimeSeriesRangeStringCheck(String badTimeSeriesRangeString) throws IOException{
        int intervalBetweenUpdates = 2;
        int threadCount = 5;
        int timeSeriesCount = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,OptionsHelper.BenchmarkModes.BATCH_INSERT,
                        intervalBetweenUpdates,threadCount,timeSeriesCount,badTimeSeriesRangeString);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("Value for %s flag should be one of <integer> followed by %s, indicating years, days, hours, minutes or seconds",
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG, OptionsHelper.TimeUnitIndicators.ALL_INDICATORS));
    }

    /**
     * Utility method checking that the TimeSeriesBenchmarker correctly converts a timeRangeString to the correct number of seconds
     * Do this by looking at the value of timeSeriesRangeSeconds and comparing to the correctly converted value - using the provided multiplier
     * @param timePart - integer part of time string
     * @param timeUnit - unit e.g. S,M,H,D,Y
     * @param multiplier - multiplier to convert to seconds
     */
    private void timeSeriesRangeParsingCheck(int timePart, String timeUnit, int multiplier) throws Utilities.ParseException{
        int intervalBetweenUpdates = 2;
        int threadCount = 5;
        int timeSeriesCount = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.MODE_FLAG,
                OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String timeSeriesRangeString = String.format("%d%s",timePart,timeUnit);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,OptionsHelper.BenchmarkModes.BATCH_INSERT,
                        intervalBetweenUpdates,threadCount,timeSeriesCount,timeSeriesRangeString);

        TimeSeriesBenchmarker benchmarker = TimeSeriesBenchmarker.initBenchmarkerFromStringArgs(commandLineArguments.split(" "));
        Assert.assertEquals(benchmarker.timeSeriesRangeSeconds, (long)timePart * multiplier);
    }

    @After
    // Truncate the time series set
    public void teardown(){
        if(doTeardown) {
            TestUtilities.removeTimeSeriesTestDataForSet(TestConstants.TIME_SERIES_TEST_SET);
        }
    }

    /**
     * Private method allowing console output to be grabbed after a benchmarker run
     * @param benchmarker Benchmarker object whose output will be captured
     * @return Console output as a vector of strings
     * @throws IOException can in theory be thrown by getConsoleOutput, but in practice it can't
     */
    private static Vector<String> runBenchmarkerGetOutput(TimeSeriesBenchmarker benchmarker) throws IOException{
        // Save the existing output stream
        PrintStream currentOut = System.out;
        // Swap stdout for an internal stream
        ByteArrayOutputStream bStream = TestUtilities.setupForConsoleOutputParsing();
        benchmarker.output = new PrintStream(bStream);
        // Run the benchmarker
        benchmarker.run();
        // Capture the output
        Vector<String> consoleOutput = TestUtilities.getConsoleOutput(bStream);
        // Replace the previous output stream
        System.setOut(currentOut);
        System.out.println("Run complete");
        return consoleOutput;
    }

}
