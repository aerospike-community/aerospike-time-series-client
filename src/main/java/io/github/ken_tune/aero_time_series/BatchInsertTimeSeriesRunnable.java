package io.github.ken_tune.aero_time_series;

import java.util.*;

public class BatchInsertTimeSeriesRunnable extends InsertTimeSeriesRunnable{
    private int requiredTimeSeriesRangeSeconds;
    private int recordsPerBlock;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     * @param asHost - Aerospike Host
     * @param asNamespace - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     */
    BatchInsertTimeSeriesRunnable(String asHost, String asNamespace, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient){
        this(asHost,asNamespace,timeSeriesCountPerObject,benchmarkClient,new Random().nextLong());
    }


    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     * @param asHost - Aerospike Host
     * @param asNamespace - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     * @param randomSeed - initialise with a specific seed for deterministic results
     */
    BatchInsertTimeSeriesRunnable(String asHost, String asNamespace, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        super(asHost, asNamespace, timeSeriesCountPerObject, benchmarkClient, randomSeed);
    }

    public void run(){
        startTime = System.currentTimeMillis();
        Map<String,Long> lastObservationTimes = new HashMap<>();
        Map<String,Double> lastObservationValues = new HashMap<>();

        // Set up each time series. Given that observation times and values are bootstrapped from the previous value we
        // have to set up what happened at 'T-1'
        for(int i = 0; i< timeSeriesCountPerObject; i++){
            String timeSeriesName = randomTimeSeriesName();
            // 'T-1'
            lastObservationTimes.put(timeSeriesName,startTime + startTime - nextObservationTime(startTime));
            // Obseration at 'T-1' - time independent so can use initTimeSeriesValue
            lastObservationValues.put(timeSeriesName,initTimeSeriesValue());
        }
        int iterations = (requiredTimeSeriesRangeSeconds / observationIntervalSeconds) / recordsPerBlock;
        isRunning = true;
        for(int iterationCount = 0;iterationCount < iterations;iterationCount++) {
            Iterator<String> timeSeriesNames = lastObservationTimes.keySet().iterator();
            while (timeSeriesNames.hasNext()) {
                String timeSeriesName = timeSeriesNames.next();
                DataPoint[] dataPoints = new DataPoint[recordsPerBlock];
                for (int i = 0; i < recordsPerBlock; i++) {
                    long lastObservationTime = (i == 0) ? lastObservationTimes.get(timeSeriesName) : dataPoints[i - 1].getTimestamp();
                    double lastObservationValue = (i == 0) ? lastObservationValues.get(timeSeriesName) : dataPoints[i - 1].getTimestamp();
                    long observationTime = nextObservationTime(lastObservationTime);
                    double timeIncrement = (double) (observationTime - lastObservationTime) / Constants.MILLISECONDS_IN_SECOND;
                    double observationValue = simulator.getNextValue(lastObservationValue, timeIncrement);
                    dataPoints[i] = new DataPoint(new Date(observationTime), observationValue);
                }
                timeSeriesClient.put(timeSeriesName, dataPoints, recordsPerBlock);
                lastObservationTimes.put(timeSeriesName, dataPoints[dataPoints.length - 1].getTimestamp());
                lastObservationValues.put(timeSeriesName, dataPoints[dataPoints.length - 1].getValue());
                updateCount += recordsPerBlock;

            }
        }
        isFinished = true;
        isRunning = false;
    }
}
