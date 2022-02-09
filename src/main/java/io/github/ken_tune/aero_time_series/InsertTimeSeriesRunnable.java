package io.github.ken_tune.aero_time_series;

/**
 * Runnable responsible for generating inserts, to be invoked by the Time Series Benchmarker
 * Adds in the observation interval and variability
 */
public abstract class InsertTimeSeriesRunnable extends TimeSeriesRunnable {
    // Average time interval in seconds between successive each time series observation
    protected int observationIntervalSeconds;
    // In the simulation we introduce variability into the time of observations
    // The actual interval is +/- observationIntervalVariabilityPct and the simulation distributes actual time intervals uniformly across this range
    protected double observationIntervalVariabilityPct;
    // Data points per Aerospike object
    protected int recordsPerBlock;




    // The simulator is used for generating the time series values
    protected TimeSeriesSimulator simulator;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     * @param asHost - Aerospike Host
     * @param asNamespace - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     * @param randomSeed - initialise with a specific seed for deterministic results
     */
    protected InsertTimeSeriesRunnable(String asHost, String asNamespace, String asSet, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        super(asHost, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, randomSeed);
        this.observationIntervalSeconds = benchmarkClient.averageObservationIntervalSeconds;
        this.observationIntervalVariabilityPct = TimeSeriesBenchmarker.OBSERVATION_INTERVAL_VARIABILITY_PCT;
        this.recordsPerBlock = benchmarkClient.recordsPerBlock;
        this.simulator = new TimeSeriesSimulator(benchmarkClient.dailyDriftPct,benchmarkClient.dailyVolatilityPct,randomSeed);
    }

    /**
     * Randomly generate the next observation time for a time series observation
     * @return
     */
    protected long nextObservationTime(long lastObservationTime){
        int intervalSamplingGranularity = 1000;
        // Randomly vary the observation interval by +/- observationIntervalVariabilityPct
        // First generate the variability
        double observationVariationPct = 100  - observationIntervalVariabilityPct
                + (2 * observationIntervalVariabilityPct * random.nextInt(intervalSamplingGranularity + 1) / intervalSamplingGranularity);
        // then apply it to the average interval. Convert to milliseconds and divide by 100 as we were working in pct terms
        return lastObservationTime + (long)(observationVariationPct * observationIntervalSeconds * Constants.MILLISECONDS_IN_SECOND/100);
    }

}
