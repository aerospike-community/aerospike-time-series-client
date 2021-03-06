package io.github.aerospike_examples.timeseries.benchmarker;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;

import java.util.Random;

/**
 * Generalised Runnable to be invoked by the Time Series Benchmarker
 * At this level, contains the data the Time Series Benchmarker will need to collect
 */
abstract class TimeSeriesRunnable implements Runnable {

    final int timeSeriesCountPerObject;

    // isRunning indicates the simulation is active
    // Set to true at initialisation time. Set to false when finished
    boolean isRunning = true;
    // Use this flag if any preparation is required
    boolean inPrepPhase = false;
    // Use this variable to keep track of how far through the prep phase we are
    double prepPhasePctComplete = 100;
    // How many inserts have been done by this thread
    int updateCount = 0;
    // Cumulative latency in milliseconds
    long cumulativeLatencyMs = 0;
    // When did the thread start - to avoid race conditions initialise
    long startTime = System.currentTimeMillis();
    // Randomness generation
    final Random random;
    // Time Series Client object
    final TimeSeriesClient timeSeriesClient;

    private final int timeSeriesNameLength;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     *
     * @param asClient        - Aerospike Client object
     * @param asNamespace     - Aerospike Namespace
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     * @param randomSeed      - initialise with a specific seed for deterministic results
     */
    TimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet, int timeSeriesCountPerObject,
                       TimeSeriesBenchmarker benchmarkClient, long randomSeed) {
        timeSeriesClient = new TimeSeriesClient(asClient, asNamespace, asSet, benchmarkClient.recordsPerBlock);
        this.timeSeriesCountPerObject = timeSeriesCountPerObject;
        this.timeSeriesNameLength = benchmarkClient.timeSeriesNameLength;
        this.random = new Random(randomSeed);
    }

    /**
     * Package level access to 'isRunning' for use by the benchmark client
     *
     * @return boolean isRunning parameter
     */
    boolean isRunning() {
        return isRunning;
    }

    /**
     * Package level access to 'inPrepPhase' for use by the benchmark client
     *
     * @return boolean inPrepPhase parameter
     */
    boolean inPrepPhase() {
        return inPrepPhase;
    }

    /**
     * Package level access to the actual run time of the thread, for use by the benchmark client
     *
     * @return runTime of thread so far, in milliseconds
     */
    long runTime() {
        return System.currentTimeMillis() - startTime;
    }

    /**
     * Package level access to the update count for the thread, for use by the benchmark client
     *
     * @return update count for thread
     */
    public int getUpdateCount() {
        return updateCount;
    }

    /**
     * Randomly generate a time series name (a String) of length timeSeriesNameLength
     * Package level visibility for testing purposes
     *
     * @return String:timeSeriesName
     */
    public String randomTimeSeriesName() {
        @SuppressWarnings("SpellCheckingInspection") char[] availableCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        char[] timeSeriesName = new char[timeSeriesNameLength];
        for (int i = 0; i < timeSeriesNameLength; i++)
            timeSeriesName[i] = availableCharacters[random.nextInt(availableCharacters.length)];
        return String.valueOf(timeSeriesName);
    }

    /**
     * Initialise the time series value
     * It will be randomly somewhere between the min and max values below
     *
     * @return initial time series value
     */
    double initTimeSeriesValue() {
        double TIME_SERIES_MIN_START_VALUE = 10.0;
        double TIME_SERIES_MAX_START_VALUE = 100.0;
        return TIME_SERIES_MIN_START_VALUE + random.nextDouble() * (TIME_SERIES_MAX_START_VALUE - TIME_SERIES_MIN_START_VALUE);
    }

}
