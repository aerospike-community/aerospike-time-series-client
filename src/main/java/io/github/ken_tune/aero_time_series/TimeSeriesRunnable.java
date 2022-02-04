package io.github.ken_tune.aero_time_series;

import java.util.Random;

/**
 * Generalised Runnable to be invoked by the Time Series Benchmarker
 * At this level, contains the data the Time Series Benchmarker will need to collect
 */
public abstract class TimeSeriesRunnable implements Runnable{
    protected int timeSeriesCountPerObject;

    // isRunning indicates the simulation is active
    // Set to true at initialisation time. Set to false when finished
    protected boolean isRunning = true;
    // Has the simulation finished (allows us to distinguish between start and end)
    protected boolean isFinished = false;
    // How many inserts have been done by this thread
    protected int updateCount = 0;
    // When did the thread start
    protected long startTime;
    // Randomness generation
    protected Random random;
    // Time Series Client object
    protected TimeSeriesClient timeSeriesClient;

    protected int timeSeriesNameLength;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     * @param asHost - Aerospike Host
     * @param asNamespace - Aerospike Namespace
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     * @param randomSeed - initialise with a specific seed for deterministic results
     */
    TimeSeriesRunnable(String asHost, String asNamespace, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        timeSeriesClient = new TimeSeriesClient(asHost,asNamespace);
        this.timeSeriesCountPerObject = timeSeriesCountPerObject;
        this.timeSeriesNameLength = benchmarkClient.timeSeriesNameLength;
        this.random = new Random(randomSeed);
    }
    /**
     * Package level access to 'isRunning' for use by the benchmark client
     * @return
     */
    boolean isRunning() {
        return isRunning;
    }

    /**
     * Package level access to the actual run time of the thread, for use by the benchmark client
     * @return
     */
    long runTime(){
        return (isFinished || isRunning()) ? System.currentTimeMillis() - startTime : 0;
    }

    /**
     * Package level access to the update count for the thread, for use by the benchmark client
     * @return
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
    String randomTimeSeriesName(){
        char[] availableCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        char[] timeSeriesName = new char[timeSeriesNameLength];
        for(int i=0;i<timeSeriesNameLength;i++) timeSeriesName[i] = availableCharacters[random.nextInt(availableCharacters.length)];
        return String.valueOf(timeSeriesName);
    }

    public double initTimeSeriesValue(){
        double TIME_SERIES_MIN_START_VALUE = 10.0;
        double TIME_SERIES_MAX_START_VALUE = 100.0;
        return TIME_SERIES_MIN_START_VALUE + random.nextDouble() * (TIME_SERIES_MAX_START_VALUE - TIME_SERIES_MIN_START_VALUE);
    }

}
