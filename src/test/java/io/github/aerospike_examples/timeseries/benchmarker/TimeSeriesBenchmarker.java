package io.github.aerospike_examples.timeseries.benchmarker;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;
import io.github.aerospike_examples.timeseries.util.Constants;
import io.github.aerospike_examples.timeseries.util.Utilities;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import java.io.PrintStream;
import java.util.Random;

/**
 * Object to facilitate time series benchmarking
 */
public class TimeSeriesBenchmarker {

    /**
     * Constant : default time series length (=10)
     */
    private static final int DEFAULT_TIME_SERIES_NAME_LENGTH = 10;

    /**
     * Constant : default time series count when creating data (=100)
     */
    public static final int DEFAULT_TIME_SERIES_COUNT = 100;

    /**
     * Constant : default period between observations when running insert benchmarks (=1)
     */
    public static final int DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS = 1;

    /**
     * Constant : default acceleration to use in real time insert mode (=1)
     */
    public static final int DEFAULT_ACCELERATION_FACTOR = 1;

    /**
     * Constant : default run duration for real time insert and query (=10)
     */
    public static final int DEFAULT_RUN_DURATION_SECONDS = 10;

    /**
     * Constant : default thread count when benchmarking (=1)
     */
    public static final int DEFAULT_THREAD_COUNT = 1;

    /**
     * Default daily volatility (%) for time series simulations (=10)
     */
    public static final int DEFAULT_DAILY_VOLATILITY_PCT = 10;

    /**
     * Default daily drift (%) for time series simulations (=10)
     */
    public static final int DEFAULT_DAILY_DRIFT_PCT = 10;

    // In the simulation we introduce variability into the time of observations
    // The actual interval is +/- observationIntervalVariabilityPct and the simulation distributes actual time intervals uniformly across this range
    public static final int OBSERVATION_INTERVAL_VARIABILITY_PCT = 5;

    // Variables controlling the simulation
    // Those with package level visibility need to be visible to the benchmark threads
    final int averageObservationIntervalSeconds;
    final int runDuration;
    final int accelerationFactor;
    public final int timeSeriesNameLength = DEFAULT_TIME_SERIES_NAME_LENGTH;
    private final int threadCount;
    private final int timeSeriesCount;
    final int recordsPerBlock;
    public final long timeSeriesRangeSeconds;
    final int dailyDriftPct;
    final int dailyVolatilityPct;
    private final String runMode;
    // Seed for initialising sources of randomness
    // If a seed is supplied in the constructor this will be used else a random seed is selected
    private final long randomSeed;

    // Specify Aerospike cluster, namespace, set
    private final String asHost;
    private final String asNamespace;
    private final String asSet;

    // Aerospike client
    @SuppressWarnings("FieldCanBeLocal")
    private AerospikeClient aerospikeClient;
    // Underlying runnable objects for the benchmark
    private TimeSeriesRunnable[] benchmarkClientObjects;
    // Output Stream
    // Give it package protection so it can be modified by unit tests
    public PrintStream output = System.out;

    // Status thread related
    @SuppressWarnings("FieldCanBeLocal")
    private static final int STATUS_UPDATE_PERIOD_SECS = 1;
    // How frequently do we check to see if a status update is needed
    @SuppressWarnings("FieldCanBeLocal")
    private static final int STATUS_TIMER_CHECK_PERIOD_MS = 50;
    // How much throughput under-performance is tolerated w/out warning
    @SuppressWarnings("FieldCanBeLocal")
    private static final int THROUGHPUT_VARIANCE_TOLERANCE_PCT = 10;


    /**
     * Benchmarker constructor
     *
     * @param asHost                     - Aerospike DB host
     * @param asNamespace                - Aerospike namespace to use when benchmarking
     * @param asSet                      - Aerospike set to use to store time series data when benchmarking
     * @param runMode                    - Benchmarker run mode
     * @param observationIntervalSeconds - Period between observations to use when generating data
     * @param runDurationSeconds         - Duration of real time or query benchmark runs
     * @param accelerationFactor         - Acceleration factor to use in real time benchmarking mode
     * @param threadCount                - No of threads to use
     * @param timeSeriesCount            - Time series count to create when running in insert mode
     * @param recordsPerBlock            - Records per block
     * @param timeSeriesRangeSeconds     - Time series range to cover when running in batch insert mode
     * @param dailyDriftPct              - Daily drift(%) to use for time series simulation
     * @param dailyVolatilityPct         - Daily volatility(%) to use for time series simulation
     * @param randomSeed                 - Random seed value to use when generating time series data. Allows data to be generated repeatedly if necessary
     */
    public TimeSeriesBenchmarker(String asHost, String asNamespace, String asSet, String runMode, int observationIntervalSeconds, int runDurationSeconds, int accelerationFactor, int threadCount,
                                 int timeSeriesCount, int recordsPerBlock, long timeSeriesRangeSeconds, int dailyDriftPct, int dailyVolatilityPct, long randomSeed) {
        this.asHost = asHost;
        this.asNamespace = asNamespace;
        this.asSet = asSet;
        this.runMode = runMode;
        this.averageObservationIntervalSeconds = observationIntervalSeconds;
        this.runDuration = runDurationSeconds;
        this.accelerationFactor = accelerationFactor;
        this.threadCount = threadCount;
        this.timeSeriesCount = timeSeriesCount;
        this.recordsPerBlock = recordsPerBlock;
        this.timeSeriesRangeSeconds = timeSeriesRangeSeconds;
        this.dailyDriftPct = dailyDriftPct;
        this.dailyVolatilityPct = dailyVolatilityPct;
        this.randomSeed = randomSeed;
    }

    /**
     * Main entry point for cmd line running of benchmarker
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        try {
            TimeSeriesBenchmarker benchmarker = initBenchmarkerFromStringArgs(args);
            benchmarker.run();
        } catch (org.apache.commons.cli.ParseException | Utilities.ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TimeSeriesBenchmarker", OptionsHelper.standardCmdLineOptions());
        }
    }

    /**
     * Helper method allowing a TimeSeriesBenchmarker to be initialised from an array of Strings - as per main method
     * Protected visibility to allow testing use
     *
     * @param args - String[] to be parsed
     * @return initialised TimeSeriesBenchmarker object
     * @throws ParseException           - if basic parsing checks fail
     * @throws Utilities.ParseException - if bespoke parsing checks fail
     */
    public static TimeSeriesBenchmarker initBenchmarkerFromStringArgs(String[] args) throws org.apache.commons.cli.ParseException, Utilities.ParseException {
        TimeSeriesBenchmarker benchmarker;

        CommandLine cmd = OptionsHelper.getArguments(args);
        CommandLineParser parser = new DefaultParser();
        // Check the command line versus options standard for all run modes
        parser.parse(OptionsHelper.standardCmdLineOptions(), args);

        benchmarker = new TimeSeriesBenchmarker(
                OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.HOST_FLAG),
                OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG),
                OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG),
                OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.MODE_FLAG),
                Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG)),
                Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG)),
                Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG)),
                Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG)),
                Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG)),
                Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.RECORDS_PER_BLOCK_FLAG)),
                OptionsHelper.convertTimeStringToSeconds(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG)),
                DEFAULT_DAILY_DRIFT_PCT,
                DEFAULT_DAILY_VOLATILITY_PCT,
                new Random().nextLong()
        );
        return benchmarker;
    }

    public void run() {
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(aerospikeClient = new AerospikeClient(asHost, Constants.DEFAULT_AEROSPIKE_PORT),
                asNamespace, asSet, recordsPerBlock);

        switch (runMode) {
            // First of all print out a header showing what is happening
            case OptionsHelper.BenchmarkModes.REAL_TIME_INSERT:
                output.println("Aerospike Time Series Benchmarker running in real time insert mode");
                output.println();
                output.println(String.format("Updates per second : %.3f", expectedUpdatesPerSecond()));
                output.println(String.format("Updates per second per time series : %.3f", updatesPerTimeSeriesPerSecond()));
                output.println();
                break;
            case OptionsHelper.BenchmarkModes.BATCH_INSERT:
                output.println("Aerospike Time Series Benchmarker running in batch insert mode");
                output.println();
                long recordCount = timeSeriesRangeSeconds / averageObservationIntervalSeconds;
                output.println(String.format("Inserting %d records per series for %d series, over a period of %d seconds", recordCount, timeSeriesCount, timeSeriesRangeSeconds));
                output.println();
                break;
            case OptionsHelper.BenchmarkModes.QUERY:
                output.println("Aerospike Time Series Benchmarker running in query mode");
                output.println();
                output.println(QueryTimeSeriesRunnable.timeSeriesInfoSummary(QueryTimeSeriesRunnable.getTimeSeriesDetails(timeSeriesClient)));
                output.println();
                break;
        }

        // If the max update rate per key exceeds the safe level, issue a warning
        if (updatesPerTimeSeriesPerSecond() > Constants.SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC) {
            output.println(String.format("!!! Single key updates per second rate %.3f exceeds max recommended rate %d",
                    updatesPerTimeSeriesPerSecond(), Constants.SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC));
        }

        // Initialisation - truncate time series if running in one of the insert modes
        if ((runMode.equals(OptionsHelper.BenchmarkModes.BATCH_INSERT) || runMode.equals(OptionsHelper.BenchmarkModes.REAL_TIME_INSERT))) {
            timeSeriesClient.getAsClient().truncate(new InfoPolicy(), asNamespace, asSet, null);
            timeSeriesClient.getAsClient().truncate(new InfoPolicy(), asNamespace, TimeSeriesClient.timeSeriesIndexSetName(asSet), null);
        }

        // Set up all the runnable objects  - based on how many threads are configured
        // and start them
        benchmarkClientObjects = new TimeSeriesRunnable[threadCount];
        Random random = new Random(randomSeed);
        for (int i = 0; i < threadCount; i++) {
            TimeSeriesRunnable runnable = null; // Keep the compiler happy with null assignment
            int timeSeriesCountForThread; // only needed for insert modes
            switch (runMode) {
                case OptionsHelper.BenchmarkModes.REAL_TIME_INSERT:
                    // Figure out how many time series per thread to manage
                    timeSeriesCountForThread = timeSeriesCount / threadCount;
                    if (i < timeSeriesCount % threadCount) timeSeriesCountForThread++;
                    runnable = new RealTimeInsertTimeSeriesRunnable(aerospikeClient, asNamespace, asSet, timeSeriesCountForThread, this, random.nextLong());
                    break;
                case OptionsHelper.BenchmarkModes.BATCH_INSERT:
                    // Figure out how many time series per thread to manage
                    timeSeriesCountForThread = timeSeriesCount / threadCount;
                    if (i < timeSeriesCount % threadCount) timeSeriesCountForThread++;
                    runnable = new BatchInsertTimeSeriesRunnable(aerospikeClient, asNamespace, asSet, timeSeriesCountForThread, this, random.nextLong());
                    break;
                case OptionsHelper.BenchmarkModes.QUERY:
                    runnable = new QueryTimeSeriesRunnable(aerospikeClient, asNamespace, asSet, 0, this, random.nextLong());
                    break;
            }
            benchmarkClientObjects[i] = runnable;
            Thread t = new Thread(benchmarkClientObjects[i]);
            t.start();
        }
        long nextOutputTime = System.currentTimeMillis();
        // While we wait for them to finish, post status messages every STATUS_UPDATE_PERIOD_SECS
        long lastUpdateCount = 0;
        double lastAverageThreadRunTimeMs = 0;
        long lastCumulativeLatencyMs = 0;
        while (isRunning()) {
            // Status message if we are due a status message
            if (System.currentTimeMillis() > nextOutputTime) {
                // But only if things have started to avoid race conditions
                if (averageThreadRunTimeMs() > 0) {
                    outputStatus(lastUpdateCount, lastAverageThreadRunTimeMs, lastCumulativeLatencyMs, false);
                    lastUpdateCount = totalUpdateCount();
                    lastAverageThreadRunTimeMs = averageThreadRunTimeMs();
                    lastCumulativeLatencyMs = totalLatencyMs();
                }
                // Set time a message is next due
                nextOutputTime += Constants.MILLISECONDS_IN_SECOND * STATUS_UPDATE_PERIOD_SECS;
                // Sleep - no point running a tight loop
                //noinspection CatchMayIgnoreException
                try {
                    Thread.sleep(STATUS_TIMER_CHECK_PERIOD_MS);
                } catch (InterruptedException e) {
                }
            }
        }
        output.println();
        output.println("Run Summary");
        output.println();
        outputStatus(lastUpdateCount, lastAverageThreadRunTimeMs, lastCumulativeLatencyMs, true);
        output.println();
    }

    /**
     * Output current status of simulation
     * Will give a warning if it is running slower than expected
     */
    private void outputStatus(long lastUpdateCount, double lastAverageThreadRunTimeMs, long lastCumulativeLatencyMs, boolean doSummary) {
        switch (runMode) {
            case OptionsHelper.BenchmarkModes.REAL_TIME_INSERT:
                outputStatusForRealTimeInserts(lastUpdateCount, lastAverageThreadRunTimeMs, doSummary);
                break;
            case OptionsHelper.BenchmarkModes.BATCH_INSERT:
                outputStatusForBatchInserts();
                break;
            case OptionsHelper.BenchmarkModes.QUERY:
                outputStatusForQueries(lastUpdateCount, lastAverageThreadRunTimeMs, lastCumulativeLatencyMs, doSummary);
                break;
        }
    }

    // Internal variable to track whether we have shown the prep complete message
    private boolean shownPrepCompleteMessage = false;

    private void outputStatusForRealTimeInserts(long lastUpdateCount, double lastAverageThreadRunTimeMs, boolean doSummary) {
        long updateCount = totalUpdateCount();
        double averageThreadRunTimeMs = averageThreadRunTimeMs();
        double updateRateSinceLastStatus = (double) Constants.MILLISECONDS_IN_SECOND * (updateCount - lastUpdateCount) / (averageThreadRunTimeMs - lastAverageThreadRunTimeMs);
        double cumulativeUpdateRate = (double) Constants.MILLISECONDS_IN_SECOND * updateCount / averageThreadRunTimeMs;
        if (!inPrepPhase()) {
            if (!shownPrepCompleteMessage) {
                output.println(String.format("In real time benchmark we prime blocks so they don't all fill at the same time. Pct complete %.3f%%", prepPhasePctComplete()));
                output.println();
                shownPrepCompleteMessage = true;
            }
            if (!doSummary) {
                output.println(String.format("Run time : %d sec, Update count : %d, Current updates/sec : %.3f, Cumulative updates/sec : %.3f",
                        averageThreadRunTimeMs() / Constants.MILLISECONDS_IN_SECOND,
                        updateCount, updateRateSinceLastStatus, cumulativeUpdateRate));
            } else
                output.println(String.format("Run time : %d sec, Update count : %d, Cumulative updates/sec : %.3f",
                        averageThreadRunTimeMs() / Constants.MILLISECONDS_IN_SECOND,
                        updateCount, cumulativeUpdateRate));

            // If the no of updates per second is *less* than expected updates per second (to a given tolerance)
            // And we are beyond the first second (can produce anomalous results )
            // show a warning message to that effect
            if ((averageThreadRunTimeMs() >= Constants.MILLISECONDS_IN_SECOND) && (expectedUpdatesPerSecond() > updateRateSinceLastStatus)) {
                if (!Utilities.valueInTolerance(expectedUpdatesPerSecond(), updateRateSinceLastStatus, THROUGHPUT_VARIANCE_TOLERANCE_PCT)) {
                    if (!doSummary) {
                        output.println(String.format("!!!Update rate should be %.3f, actually %.3f - underflow",
                                expectedUpdatesPerSecond(), updateRateSinceLastStatus));
                    }
                }
            }
        } else
            output.println(String.format("In real time benchmark we prime blocks so they don't all fill at the same time. Pct complete %.3f%%", prepPhasePctComplete()));

    }

    private void outputStatusForBatchInserts() {
        long expectedUpdateCount = timeSeriesCount * timeSeriesRangeSeconds / averageObservationIntervalSeconds;
        double pctComplete = 100 * (double) totalUpdateCount() / expectedUpdateCount;
        output.println(String.format("Run time : %d sec, Data point insert count : %d, Effective updates/sec : %.3f. Pct complete %.3f%%", averageThreadRunTimeMs() / Constants.MILLISECONDS_IN_SECOND,
                totalUpdateCount(), (double) Constants.MILLISECONDS_IN_SECOND * totalUpdateCount() / averageThreadRunTimeMs(), pctComplete));
    }

    private void outputStatusForQueries(long lastQueryCount, double lastAverageThreadRunTimeMs, long lastCumulativeLatencyMs, boolean doSummary) {
        long queryCount = totalUpdateCount();
        double averageThreadRunTimeMs = averageThreadRunTimeMs();
        long cumulativeLatencyMs = totalLatencyMs();
        double queryRateSinceLastStatus = (double) Constants.MILLISECONDS_IN_SECOND * (queryCount - lastQueryCount) / (averageThreadRunTimeMs - lastAverageThreadRunTimeMs);
        double cumulativeQueryRate = (double) Constants.MILLISECONDS_IN_SECOND * queryCount / averageThreadRunTimeMs;
        double avgLatency = (queryCount != 0) ? (double) cumulativeLatencyMs / queryCount / Constants.MILLISECONDS_IN_SECOND : 0;
        double latencySinceLastStatus = (queryCount != lastQueryCount) ?
                (double) ((cumulativeLatencyMs - lastCumulativeLatencyMs) / (queryCount - lastQueryCount)) / Constants.MILLISECONDS_IN_SECOND : 0;

        if (!doSummary) {
            output.println(String.format("Run time : %d sec, Query count : %d, Current queries/sec %.3f, Current latency %.3fs, Avg latency %.3fs, Cumulative queries/sec %.3f",
                    averageThreadRunTimeMs() / Constants.MILLISECONDS_IN_SECOND, queryCount, queryRateSinceLastStatus,
                    latencySinceLastStatus, avgLatency, cumulativeQueryRate));
        } else
            output.println(String.format("Run time : %d sec, Query count : %d, Cumulative queries/sec %.3f, Avg latency %.3fs",
                    averageThreadRunTimeMs() / Constants.MILLISECONDS_IN_SECOND, queryCount, cumulativeQueryRate, avgLatency));
    }

    /**
     * Is the simulation still running - check each of the threads
     *
     * @return True if at least one thread is still running, else false
     */
    private boolean isRunning() {
        boolean running = false;
        for (TimeSeriesRunnable benchmarkClientObject : benchmarkClientObjects) {
            running |= benchmarkClientObject.isRunning();
        }
        return running;
    }

    /**
     * Is the simulation still running - check each of the threads
     *
     * @return True if at least one thread is still running, else false
     */
    private boolean inPrepPhase() {
        boolean inPrepPhase = false;
        for (TimeSeriesRunnable benchmarkClientObject : benchmarkClientObjects) {
            inPrepPhase |= benchmarkClientObject.inPrepPhase();
        }
        return inPrepPhase;
    }

    private double prepPhasePctComplete() {
        double prepPhasePctCompleteCumulative = 0;
        for (TimeSeriesRunnable benchmarkClientObject : benchmarkClientObjects) {
            prepPhasePctCompleteCumulative += benchmarkClientObject.prepPhasePctComplete;
        }
        return prepPhasePctCompleteCumulative / benchmarkClientObjects.length;
    }

    /**
     * Compute the simulation duration by looking at the average thread run time
     * Return value is in milliseconds
     * Package level visibility for testing purposes
     *
     * @return average thread run time in milliseconds
     */
    public long averageThreadRunTimeMs() {
        long runTime = 0;
        for (TimeSeriesRunnable benchmarkClientObject : benchmarkClientObjects) {
            runTime += benchmarkClientObject.runTime();
        }
        return runTime / benchmarkClientObjects.length;
    }

    /**
     * Compute the total number of inserts for the simulation
     * Do this by aggregating inserts per thread
     * Package level visibility for testing purposes*
     *
     * @return total number of updates
     */
    public int totalUpdateCount() {
        int totalUpdateCount = 0;
        for (TimeSeriesRunnable benchmarkClientObject : benchmarkClientObjects) {
            totalUpdateCount += benchmarkClientObject.getUpdateCount();
        }
        return totalUpdateCount;
    }

    /**
     * Get the total latency for the operations, so we can figure out average latency
     *
     * @return total latency so far
     */
    private long totalLatencyMs() {
        int totalLatencyMs = 0;
        for (TimeSeriesRunnable benchmarkClientObject : benchmarkClientObjects) {
            totalLatencyMs += benchmarkClientObject.cumulativeLatencyMs;
        }
        return totalLatencyMs;
    }

    /**
     * Work out the actual number of updates per second for the simulation - allowing for the acceleration
     * Package level access to allow for testing
     *
     * @return total expected updates per second
     */
    public double expectedUpdatesPerSecond() {
        return (double) accelerationFactor * timeSeriesCount / averageObservationIntervalSeconds;
    }

    /**
     * Actual number of updates per time series per second - allowing for the acceleration
     *
     * @return updates per time series per second
     */
    private double updatesPerTimeSeriesPerSecond() {
        return (double) accelerationFactor / averageObservationIntervalSeconds;
    }

}
