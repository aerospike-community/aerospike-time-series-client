package io.github.aerospike_examples.timeseries.benchmarker;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.benchmarker.util.ClientUtils;
import io.github.aerospike_examples.timeseries.util.Constants;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

/**
 * This runnable will insert data in real time for a specified period
 * It is possible to 'accelerate' time in order to generate inserts at a faster rate
 */
public class RealTimeInsertTimeSeriesRunnable extends InsertTimeSeriesRunnable {

    private final int runDurationSeconds;

    private final int accelerationFactor;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     *
     * @param asClient                 - Aerospike client object
     * @param asNamespace              - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient          - Initialise with a benchmarkClient object - some of the config is taken from this
     */
    @SuppressWarnings("SameParameterValue")
    public RealTimeInsertTimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet,
                                            int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient) {
        this(asClient, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, new Random().nextLong());
        // We need a prep phase
        inPrepPhase = true;
        prepPhasePctComplete = 0;
    }

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
    public RealTimeInsertTimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet,
                                            int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed) {
        super(asClient, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, randomSeed);
        this.runDurationSeconds = benchmarkClient.runDuration;
        this.accelerationFactor = benchmarkClient.accelerationFactor;
        // We need a prep phase
        inPrepPhase = true;
        prepPhasePctComplete = 0;
    }

    public void run() {
        startTime = System.currentTimeMillis();
        Map<String, Long> lastObservationTimes = new HashMap<>();
        Map<String, Long> nextObservationTimes = new HashMap<>();
        Map<String, Double> lastObservationValues = new HashMap<>();

        Vector<String> timeSeriesNames = new Vector<>();
        for (int i = 0; i < timeSeriesCountPerObject; i++) timeSeriesNames.add(randomTimeSeriesName());

        /*
            Put some dummy data in when running in real time mode
            If we don't all the blocks will fill up at the same time which creates a sawtooth effect
            as far as disk use is concerned
            So blocks are primed initially, and the dummy data is removed at the end of the run
         */
        prepPhasePctComplete = 0;
        for (String timeSeriesName : timeSeriesNames) {
            int epochTime = 0;
            // Randomly each initial block to a random extent
            int dummyRecordCount = random.nextInt(timeSeriesClient.getMaxBlockEntryCount());
            DataPoint[] dataPoints = new DataPoint[dummyRecordCount];
            for (int i = 0; i < dummyRecordCount; i++) {
                // The data points have -ve time and value zero, so they are easily identified
                dataPoints[i] = new DataPoint(new Date(epochTime), 0);
                epochTime -= Constants.MILLISECONDS_IN_SECOND;
            }
            timeSeriesClient.put(timeSeriesName, dataPoints);
            // Bump the start time here so we don't suddenly go backwards
            startTime = System.currentTimeMillis();
            // Bump pct complete
            prepPhasePctComplete += 100.0 / timeSeriesNames.size();
        }
        inPrepPhase = false;

        // Initialise stored values
        for (String timeSeriesName : timeSeriesNames) {
            double observationValue = initTimeSeriesValue();
            lastObservationTimes.put(timeSeriesName, startTime);
            lastObservationValues.put(timeSeriesName, observationValue);
            timeSeriesClient.put(timeSeriesName, new DataPoint(new Date(startTime), observationValue));
            nextObservationTimes.put(timeSeriesName, nextObservationTime(startTime));
        }

        while (getSimulationTime() - startTime < (long) runDurationSeconds * Constants.MILLISECONDS_IN_SECOND * accelerationFactor) {
            for (String timeSeriesName : nextObservationTimes.keySet()) {
                long nextObservationTime = nextObservationTimes.get(timeSeriesName);
                if (nextObservationTime < getSimulationTime()) {
                    updateCount++;
                    double timeIncrement = (double) (nextObservationTime - lastObservationTimes.get(timeSeriesName)) / Constants.MILLISECONDS_IN_SECOND;
                    double observationValue = simulator.getNextValue(lastObservationValues.get(timeSeriesName), timeIncrement);
                    timeSeriesClient.put(timeSeriesName, new DataPoint(new Date(nextObservationTime), observationValue));
                    lastObservationValues.put(timeSeriesName, observationValue);
                    lastObservationTimes.put(timeSeriesName, nextObservationTime);
                    nextObservationTimes.put(timeSeriesName, nextObservationTime(nextObservationTime));
                }
            }
        }
        isRunning = false;

        // Then remove the dummy records
        for (String timeSeriesName : lastObservationTimes.keySet()) {
            ClientUtils.removeDummyRecords(timeSeriesClient, timeSeriesName);
        }
    }

    /**
     * Time may be 'sped up' during our simulation via use of the acceleration factor parameter
     * This function returns the accelerated simulation time represented as a unix epoch, in milliseconds
     *
     * @return current 'simulation time', factoring in acceleration
     */
    private long getSimulationTime() {
        return startTime + (System.currentTimeMillis() - startTime) * accelerationFactor;
    }

}
