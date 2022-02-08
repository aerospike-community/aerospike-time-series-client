package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import org.apache.commons.cli.ParseException;
import org.junit.*;

import java.io.*;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BenchmarkerTest {
    private boolean doTeardown = false;

    // For the avoidance of doubt and clarity
    int MILLISECONDS_IN_SECOND = 1000;

    @Test
    /**
     * Check that the randomly generated time series name is of the expected length
     */
    public void checkTimeSeriesNameGeneration(){
        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                        TimeSeriesBenchmarker.DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS, TimeSeriesBenchmarker.DEFAULT_RUN_DURATION_SECONDS,
                        TimeSeriesBenchmarker.DEFAULT_ACCELERATION_FACTOR, TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT,
                        TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT);

        RealTimeInsertTimeSeriesRunnable benchmarkRunnable = new RealTimeInsertTimeSeriesRunnable(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,1,benchmarker);
        String timeSeriesName = benchmarkRunnable.randomTimeSeriesName();
        Assert.assertTrue(timeSeriesName.length() == benchmarker.timeSeriesNameLength);
    }

    @Test
    /**
     * Check that time series name generation is random
     * 10,000 samples - are any identical
     */
    public void checkTimeSeriesNamesUnique(){
        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                        TimeSeriesBenchmarker.DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS, TimeSeriesBenchmarker.DEFAULT_RUN_DURATION_SECONDS,
                        TimeSeriesBenchmarker.DEFAULT_ACCELERATION_FACTOR, TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT,
                        TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT);

        RealTimeInsertTimeSeriesRunnable benchmarkRunnable = new RealTimeInsertTimeSeriesRunnable(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,1,benchmarker);

        int randomSampleCount = 10000;
        Set<String> s = new HashSet<>();
        for(int i=0;i<randomSampleCount;i++) s.add(benchmarkRunnable.randomTimeSeriesName());
        Assert.assertTrue(s.size() == randomSampleCount);
    }

    /**
     * Basic test to make sure that the benchmarker runs for the expected amount of time and delivers expected updates
     */
    @Test
    public void checkVanillaBenchmarkDurationAndUpdates(){
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 1;
        int threadCount = 1;
        int timeSeriesCount = 1;
        // Set test tolerance to 20% as we are using small numbers - variability could be slightly higher than 10% at any given time
        int testTolerancePct = 20;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                intervalBetweenUpdates, runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * MILLISECONDS_IN_SECOND,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
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

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                        intervalBetweenUpdates,runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * MILLISECONDS_IN_SECOND,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
    }

    /**
     * Check that thread count can be set to value other than 1 and still get correct results
     * This can be seen via the number of updates
     */
    @Test
    public void checkTimeSeriesCountObserved(){
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 5;
        int threadCount = 5;
        int timeSeriesCount = 100;
        int testTolerancePct = 5;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,
                TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * MILLISECONDS_IN_SECOND,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
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

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                intervalBetweenUpdates,runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * MILLISECONDS_IN_SECOND,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount,benchmarker.totalUpdateCount(),testTolerancePct));
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

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                intervalBetweenUpdates,runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        benchmarker.run();
        // Check that run time is within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(runDurationSeconds * MILLISECONDS_IN_SECOND,benchmarker.averageThreadRunTimeMs(),testTolerancePct));
        // Check that expected updates are within tolerance
        Assert.assertTrue(Utilities.valueInTolerance(accelerationFactor * runDurationSeconds * timeSeriesCount / intervalBetweenUpdates,
                benchmarker.totalUpdateCount(),testTolerancePct));
    }

    /**
     * If we exceed the number of key updates we can accommodate per second, is there a warning message?
     */
    @Test
    public void checkHotKeyCheck() throws Exception{
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 101;
        int threadCount = 1;
        int timeSeriesCount = 1;
        double updatesPerSecond = (double) accelerationFactor * timeSeriesCount / intervalBetweenUpdates;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,
                TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        Vector<String> consoleOutput = runBenchmarkerGetOutput(benchmarker);
        Assert.assertTrue(consoleOutput.get(2).equals(
                String.format("!!! Single key updates per second rate %.3f exceeds max recommended rate %d",updatesPerSecond,Constants.SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC)));
    }

    /**
     * Check drift is simulated correctly
     */
    @Test
    public void checkDriftSimulatedCorrectly(){
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
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,
                        intervalBetweenUpdates,runDurationSeconds,accelerationFactor,
                        threadCount,timeSeriesCount,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK,
                        0,dailyDriftPct,dailyVolatilityPct, TestConstants.RANDOM_SEED);
        long startTime = System.currentTimeMillis();
        benchmarker.run();
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE);
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TestConstants.REFERENCE_TIME_SERIES_NAME,new Date(startTime),new Date(startTime + (long)accelerationFactor * runDurationSeconds * 1000));
        double[] values = new double[dataPoints.length];
        for(int i=0;i<values.length;i++) values[i] = dataPoints[i].getValue();
        TimeSeriesSimulatorTest.checkDailyDriftPct(values,dailyDriftPct,intervalBetweenUpdates,10);
    }

    /**
     * Check volatility is simulated correctly
     */
    @Test
    public void checkVolatilitySimulatedCorrectly(){
        // Generate 500 observations via acceleration
        int intervalBetweenUpdates = 60;
        int runDurationSeconds = 10;
        int accelerationFactor = intervalBetweenUpdates *50;
        int threadCount = 1;
        int timeSeriesCount = 1;
        // Set drift to zero so we can focus on volatility
        int dailyDriftPct = 0;
        int dailyVolatilityPct = 10;
        // Check the simulation generates drift correctly when volatility is zero
        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE, OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,
                        intervalBetweenUpdates,runDurationSeconds,accelerationFactor,
                        threadCount,timeSeriesCount,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK,0,dailyDriftPct,dailyVolatilityPct, TestConstants.RANDOM_SEED);
        long startTime = System.currentTimeMillis();
        benchmarker.run();
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE);
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TestConstants.REFERENCE_TIME_SERIES_NAME,new Date(startTime),new Date(startTime + (long)accelerationFactor * runDurationSeconds * 1000));
        double[] values = new double[dataPoints.length];
        for(int i=0;i<values.length;i++) values[i] = dataPoints[i].getValue();
        TimeSeriesSimulatorTest.checkDailyVolatilityPct(values,dailyVolatilityPct,intervalBetweenUpdates,10);
    }

    @Test
    /**
     * Check that we get a warning message if the simulation is underflowing the expected rate
     */
    public void checkUnderflowCheck() throws Exception{
        int intervalBetweenUpdates = 1;
        int runDurationSeconds = 10;
        int accelerationFactor = 100;
        int threadCount = 1;
        int timeSeriesCount = 100;

        TimeSeriesBenchmarker benchmarker = TestUtilities.realTimeInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                intervalBetweenUpdates,runDurationSeconds,accelerationFactor, threadCount,timeSeriesCount);

        Vector<String> consoleOutput = runBenchmarkerGetOutput(benchmarker);
        int warningCount = 0;
        for(int i=0;i<consoleOutput.size();i++){
            System.out.println(consoleOutput.get(i));
            if(consoleOutput.get(i).startsWith(
                    String.format("!!!Update rate should be %.3f, actually",benchmarker.expectedUpdatesPerSecond())) && consoleOutput.get(i).endsWith(" - underflow")) warningCount++;
        }
        // We should get a warning on every second, for rounding reasons the first two may not happen
        Assert.assertTrue(warningCount >= runDurationSeconds -2);
    }


    @Test
    /**
     * Check output is as expected for a test case
     * Should see updates per second / updates per second per time series header
     * Should see a status message every second plus an initial status message
     */
    public void correctMainOutput() throws IOException, ParseException, Utilities.ParseException{
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

        // Check the two header messages
        Assert.assertTrue(consoleOutput.get(0).equals(
                String.format("Updates per second : %.3f",(double)accelerationFactor * timeSeriesCount / intervalBetweenUpdates)));
        Assert.assertTrue(consoleOutput.get(1).equals(
                String.format("Updates per second per time series : %.3f",(double)accelerationFactor * timeSeriesCount / intervalBetweenUpdates / timeSeriesCount)));

        // Check we get the expected number of status messages
        Pattern pattern = Pattern.compile("Run time :  \\d+ seconds, Update count : \\d+, Actual updates per second : \\d+.\\d{3}", Pattern.CASE_INSENSITIVE);

        int runTimeMessageCount = 0;
        for(int i=0;i<consoleOutput.size();i++){
            Matcher matcher = pattern.matcher(consoleOutput.get(i));
            if(matcher.find()) runTimeMessageCount++;
        }

        Assert.assertTrue(runTimeMessageCount >= runDurationSeconds+1);
    }

    @Test
    /**
     * Check that when running in batch mode
     * 1) The expected number of data points are created
     * 2) The number of 'blocks' is as expected
     * 3) The drift and variance of the resulting series is as expected
     */
    public void batchModeCheck() throws IOException, ParseException, Utilities.ParseException {
        int intervalBetweenUpdates = 10;
        int threadCount = 1;
        int timeSeriesCount = 1;
        int recordsPerBlock = 500;
        int timeSeriesRangeSeconds = 86400;

        // Keep track of the start time - useful when we retrieve the data points
        long startTime = System.currentTimeMillis();

        TimeSeriesBenchmarker benchmarker = TestUtilities.batchInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,intervalBetweenUpdates,timeSeriesRangeSeconds,
                threadCount,timeSeriesCount,recordsPerBlock, TestConstants.RANDOM_SEED);

        benchmarker.run();

        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE);
        // Widen the range slightly to allow for the fact that the benchmarker introduces variability in the sample times of OBSERVATION_INTERVAL_VARIABILITY_PCT
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TestConstants.REFERENCE_TIME_SERIES_NAME,new Date(startTime - Constants.MILLISECONDS_IN_SECOND),
                new Date(startTime + (1+ timeSeriesRangeSeconds)  * Constants.MILLISECONDS_IN_SECOND));

        // Check that the number of points stored is as expected
        Assert.assertTrue(timeSeriesRangeSeconds / intervalBetweenUpdates == dataPoints.length);

        // Check that the number of blocks stored is as expected
        int timeSeriesBlocks = TestUtilities.blockCountForTimeseries(timeSeriesClient.asClient,TestConstants.AEROSPIKE_NAMESPACE,TestConstants.REFERENCE_TIME_SERIES_NAME);
        int expectedTimeSeriesBlocks = (int)Math.ceil((double)timeSeriesRangeSeconds / (recordsPerBlock * intervalBetweenUpdates));
        Assert.assertTrue(timeSeriesBlocks == expectedTimeSeriesBlocks);

        // Check that the drift and variance are as expected
        double[] values = new double[dataPoints.length];
        for(int i=0;i<values.length;i++) values[i] = dataPoints[i].getValue();

        TimeSeriesSimulatorTest.checkDailyDriftPct(values,TimeSeriesBenchmarker.DEFAULT_DAILY_DRIFT_PCT,intervalBetweenUpdates,20);
        TimeSeriesSimulatorTest.checkDailyVolatilityPct(values,TimeSeriesBenchmarker.DEFAULT_DAILY_VOLATILITY_PCT,intervalBetweenUpdates,10);

    }

    @Test
    /**
     * Check parse exception is well handled
     */
    public void cmdLineParseExceptionHandled() throws IOException, ParseException, Utilities.ParseException{
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

        Vector<String> consoleOutput = runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertTrue(consoleOutput.get(0).equals(
                String.format("-%s flag should have an integer argument. Argument supplied is %s",OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG,badThreadCount)));
    }

    @Test
    /**
     * Check time series range flag presence triggers an error if found in real time insert invocation
     */
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

        Vector<String> consoleOutput = runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertTrue(consoleOutput.get(0).equals(String.format("-%s flag (%s) should not be used in %s mode",OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,
                OptionsHelper.cmdLineOptionsForRealTimeInsert().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.REAL_TIME_INSERT)));
    }

    @Test
    /**
     * Check run duration flag presence triggers an error if found in batch insert invocation
     */
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

        Vector<String> consoleOutput = runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertTrue(consoleOutput.get(0).equals(String.format("-%s flag (%s) should not be used in %s mode",OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG,
                OptionsHelper.cmdLineOptionsForBatchInsert().getOption(OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.BATCH_INSERT)));
    }

    @Test
    /**
     * Check acceleration flag presence triggers an error if found in batch insert invocation
     */
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

        Vector<String> consoleOutput = runBenchmarkerGetOutput(commandLineArguments);
        System.out.println(consoleOutput.get(0));

        Assert.assertTrue(consoleOutput.get(0).equals(String.format("-%s flag (%s) should not be used in %s mode",OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,
                        OptionsHelper.cmdLineOptionsForBatchInsert().getOption(OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG).getLongOpt(),
                        OptionsHelper.BenchmarkModes.BATCH_INSERT)));
    }

    @Test
    /**
     * Check bad run modes are handled well
     */
    public void badModeHandling() throws IOException, ParseException, Utilities.ParseException{
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

        Vector<String> consoleOutput = runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertTrue(consoleOutput.get(0).equals(
                String.format("%s is an invalid run mode. Please use %s or %s",badMode,
                        OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,OptionsHelper.BenchmarkModes.BATCH_INSERT)));
    }

    @Test
    /**
     * Check time series range is correctly converted for each possible suffix option
     */
    public void timeSeriesRangeConversionCheck() throws IOException, Utilities.ParseException{
        timeSeriesRangeParsingCheck(20,OptionsHelper.TimeUnitIndicators.SECOND,1);
        timeSeriesRangeParsingCheck(45,OptionsHelper.TimeUnitIndicators.MINUTE,60);
        timeSeriesRangeParsingCheck(12,OptionsHelper.TimeUnitIndicators.HOUR,60*60);
        timeSeriesRangeParsingCheck(5,OptionsHelper.TimeUnitIndicators.DAY,24*60*60);
        timeSeriesRangeParsingCheck(3,OptionsHelper.TimeUnitIndicators.YEAR,365*24*60*60);
        timeSeriesRangeParsingCheck(20,"",1);

    }

    @Test
    /**
     * Check time series range format value matches number followed by one of Y,D,H,M,S or no unit
     * and that we throw an error if not
     */
    public void badTimeSeriesRange() throws IOException, Utilities.ParseException{
        badTimeSeriesRangeStringCheck("50X");
        badTimeSeriesRangeStringCheck("X50");
    }

    /**
     * Private function that checks if a given 'bad' time series range string gets the expected error message
     * @param badTimeSeriesRangeString
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

        Vector<String> consoleOutput = runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertTrue(consoleOutput.get(0).equals(
                String.format("Value for %s flag should be one of <integer> followed by %s, indicating years, days, hours, minutes or seconds",
                        OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG, OptionsHelper.TimeUnitIndicators.ALL_INDICATORS)));
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
        Assert.assertTrue(benchmarker.timeSeriesRangeSeconds == timePart * multiplier);
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

    /**
     * Private method allowing console output to be grabbed after a benchmarker run
     * @param benchmarker
     * @return Console output as a vector of strings
     * @throws IOException
     */
    private static Vector<String> runBenchmarkerGetOutput(TimeSeriesBenchmarker benchmarker) throws IOException{
        // Save the existing output stream
        PrintStream currentOut = System.out;
        // Swap stdout for an internal stream
        ByteArrayOutputStream bStream = setupForConsoleOutputParsing();
        benchmarker.output = new PrintStream(bStream);
        // Run the benchmarker
        benchmarker.run();
        // Capture the output
        Vector<String> consoleOutput = getConsoleOutput(bStream);
        // Replace the previous output stream
        System.setOut(currentOut);
        System.out.println("Run complete");
        return consoleOutput;
    }

    /**
     * Private method for setting up environment so we can test the console output
     * The returned stream is needed to process the console output
     *
     * @return ByteArrayOutputStream
     */
    private static ByteArrayOutputStream setupForConsoleOutputParsing(){
        System.out.println("This test requires console output to be captured. Running ....");
        // Capture existing stdout
        PrintStream oldSout = System.out;
        // Create new stdout
        ByteArrayOutputStream bStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bStream));
        return bStream;
    }

    /**
     * Private method for getting the console output as a vector of strings
     * Requires a previously defined output stream
     * @param bStream
     * @return
     */
    private static Vector<String> getConsoleOutput(ByteArrayOutputStream bStream) throws IOException{
        // Convert stdout into a vector of strings
        ByteArrayInputStream binStream = new ByteArrayInputStream(bStream.toByteArray());
        BufferedReader reader =  new BufferedReader(new InputStreamReader(binStream));
        Vector<String> consoleOutput = new Vector<>();
        while(reader.ready()) consoleOutput.addElement(reader.readLine());
        return consoleOutput;
    }

    private static Vector<String> runBenchmarkerGetOutput(String commandLineArguments) throws IOException{
        // Save the existing output stream
        PrintStream currentOut = System.out;
        // Swap in a new output stream
        ByteArrayOutputStream consoleOutputStream = setupForConsoleOutputParsing();
        // Run benchmarker
        TimeSeriesBenchmarker.main(commandLineArguments.split(" "));
        // Capture the output
        Vector<String> consoleOutput = getConsoleOutput(consoleOutputStream);
        // Replace the previous output stream
        System.setOut(currentOut);
        System.out.println("Run complete");
        return consoleOutput;
    }
}
