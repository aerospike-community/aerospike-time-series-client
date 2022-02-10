package io.github.aerospike_examples.aero_time_series.benchmark;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.client.DataPoint;

import java.util.*;

class BatchInsertTimeSeriesRunnable extends InsertTimeSeriesRunnable {
    private final long requiredTimeSeriesRangeSeconds;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     * @param asClient - Aerospike Client object
     * @param asNamespace - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     * @param randomSeed - initialise with a specific seed for deterministic results
     */
    BatchInsertTimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        super(asClient, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, randomSeed);
        recordsPerBlock = benchmarkClient.recordsPerBlock;
        requiredTimeSeriesRangeSeconds = benchmarkClient.timeSeriesRangeSeconds;
    }

    public void run(){
        startTime = System.currentTimeMillis();
        Map<String,Long> lastObservationTimes = new HashMap<>();
        Map<String,Double> lastObservationValues = new HashMap<>();
        Map<String,Integer> recordCountPerSeries = new HashMap<>();

        // Set up each time series. Given that observation times and values are bootstrapped from the previous value we
        // have to set up what happened at 'T-1'
        for(int i = 0; i< timeSeriesCountPerObject; i++){
            String timeSeriesName = randomTimeSeriesName();
            // 'T-1'
            lastObservationTimes.put(timeSeriesName,startTime + startTime - nextObservationTime(startTime));
            // Observation at 'T-1' - time independent so can use initTimeSeriesValue
            lastObservationValues.put(timeSeriesName,initTimeSeriesValue());
            recordCountPerSeries.put(timeSeriesName,0);
        }
        long recordsToInsertPerSeries = requiredTimeSeriesRangeSeconds / observationIntervalSeconds;
        int iterations = (int)Math.ceil(((double)requiredTimeSeriesRangeSeconds / observationIntervalSeconds) / recordsPerBlock);
        long maxTimestamp = startTime + requiredTimeSeriesRangeSeconds * Constants.MILLISECONDS_IN_SECOND;
        isRunning = true;

        for(int iterationCount = 0;iterationCount < iterations;iterationCount++) {
            for (String timeSeriesName : lastObservationTimes.keySet()) {
                long maxRecordsToInsert = Math.min(recordsPerBlock, recordsToInsertPerSeries - recordCountPerSeries.get(timeSeriesName));
                Vector<DataPoint> dataPointVector = new Vector<>();
                int recordsInCurrentBatch = 0;
                while (recordsInCurrentBatch < maxRecordsToInsert) {
                    long lastObservationTime = (recordsInCurrentBatch == 0) ? lastObservationTimes.get(timeSeriesName) : dataPointVector.get(recordsInCurrentBatch - 1).getTimestamp();
                    double lastObservationValue = (recordsInCurrentBatch == 0) ? lastObservationValues.get(timeSeriesName) : dataPointVector.get(recordsInCurrentBatch - 1).getValue();
                    long observationTime = nextObservationTime(lastObservationTime);
                    if (observationTime > maxTimestamp) break;
                    double timeIncrement = (double) (observationTime - lastObservationTime) / Constants.MILLISECONDS_IN_SECOND;
                    double observationValue = simulator.getNextValue(lastObservationValue, timeIncrement);
                    dataPointVector.add(new DataPoint(new Date(observationTime), observationValue));
                    recordsInCurrentBatch++;
                }
                DataPoint[] dataPoints = dataPointVector.toArray(new DataPoint[0]);
                timeSeriesClient.put(timeSeriesName, dataPoints);
                lastObservationTimes.put(timeSeriesName, dataPoints[dataPoints.length - 1].getTimestamp());
                lastObservationValues.put(timeSeriesName, dataPoints[dataPoints.length - 1].getValue());
                recordCountPerSeries.put(timeSeriesName, recordCountPerSeries.get(timeSeriesName) + recordsInCurrentBatch);
                updateCount += recordsInCurrentBatch;
            }
        }
        isFinished = true;
        isRunning = false;
    }
}
