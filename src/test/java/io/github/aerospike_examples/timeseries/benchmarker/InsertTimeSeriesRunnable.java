package io.github.aerospike_examples.timeseries.benchmarker;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.util.Constants;

/**
 * Runnable responsible for generating inserts, to be invoked by the Time Series Benchmarker
 * Adds in the observation interval and variability
 */
abstract class InsertTimeSeriesRunnable extends TimeSeriesRunnable {

    // Average time interval in seconds between successive each time series observation
    final int observationIntervalSeconds;

    // In the simulation we introduce variability into the time of observations
    // The actual interval is +/- observationIntervalVariabilityPct and the simulation distributes actual time intervals
    // uniformly across this range
    private final double observationIntervalVariabilityPct;

    // Data points per Aerospike object
    int recordsPerBlock;

    // The simulator is used for generating the time series values
    final TimeSeriesSimulator simulator;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     *
     * @param asClient                 - Aerospike Client object
     * @param asNamespace              - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient          - Initialise with a benchmarkClient object - some of the config is taken from this
     * @param randomSeed               - initialise with a specific seed for deterministic results
     */
    InsertTimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet, int timeSeriesCountPerObject,
                             TimeSeriesBenchmarker benchmarkClient, long randomSeed) {
        super(asClient, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, randomSeed);
        this.observationIntervalSeconds = benchmarkClient.averageObservationIntervalSeconds;
        this.observationIntervalVariabilityPct = TimeSeriesBenchmarker.OBSERVATION_INTERVAL_VARIABILITY_PCT;
        this.recordsPerBlock = benchmarkClient.recordsPerBlock;
        this.simulator = new TimeSeriesSimulator(benchmarkClient.dailyDriftPct, benchmarkClient.dailyVolatilityPct, randomSeed);
    }

    /**
     * Randomly generate the next observation time for a time series observation
     * Random variation in time interval is +/- observationIntervalVariabilityPct
     *
     * @return next observation time
     */
    long nextObservationTime(long lastObservationTime) {
        int intervalSamplingGranularity = 1000;
        // Randomly vary the observation interval by +/- observationIntervalVariabilityPct
        // First generate the variability
        double observationVariationPct = 100 - observationIntervalVariabilityPct
                + (2 * observationIntervalVariabilityPct * random.nextInt(intervalSamplingGranularity + 1) / intervalSamplingGranularity);
        // then apply it to the average interval. Convert to milliseconds and divide by 100 as we were working in pct terms
        return lastObservationTime + (long) (observationVariationPct * observationIntervalSeconds * Constants.MILLISECONDS_IN_SECOND / 100);
    }

}
