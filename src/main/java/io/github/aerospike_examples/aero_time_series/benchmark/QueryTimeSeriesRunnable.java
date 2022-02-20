package io.github.aerospike_examples.aero_time_series.benchmark;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesInfo;

import java.util.Date;
import java.util.Vector;

/**
 * This class is a runnable for running time series queries in order to benchmark query performance
 */
class QueryTimeSeriesRunnable extends TimeSeriesRunnable{
    private final TimeSeriesClient timeSeriesClient;
    private final long runDurationSeconds;
    private static Vector<TimeSeriesInfo> timeSeriesInfoList = null;

    public QueryTimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        super(asClient, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, randomSeed);
        timeSeriesClient = new TimeSeriesClient(asClient,asNamespace,asSet,timeSeriesCountPerObject);
        runDurationSeconds = benchmarkClient.runDuration;
    }

    /**
     * Get a list of all time series
     * While the run time is within required limits
     * Run a query vs a randomly selected time series
     * The query will calculate the average value of the full time series history
     */
    public void run(){
        // Need to know when we started to get metrics
        startTime = System.currentTimeMillis();
        // Need the Time series info details
        Vector<TimeSeriesInfo> timeSeriesInfoList = getTimeSeriesDetails(timeSeriesClient);
        // Loop until run time exceeds run duration seconds
        while(System.currentTimeMillis() - startTime < runDurationSeconds * Constants.MILLISECONDS_IN_SECOND){
            // Randomly select time series
            TimeSeriesInfo timeSeriesInfo = timeSeriesInfoList.get(random.nextInt(timeSeriesInfoList.size()));
            // When did the query start
            long queryRunTimeStart = System.currentTimeMillis();
            timeSeriesClient.runQuery(timeSeriesInfo.getSeriesName(),TimeSeriesClient.QueryOperation.AVG,
                    new Date(timeSeriesInfo.getStartDateTime()),new Date(timeSeriesInfo.getEndDateTime()));
            // Add total query time to cumulative latency
            cumulativeLatencyMs+= System.currentTimeMillis() - queryRunTimeStart;
            updateCount++;
        }
        isRunning = false;

    }

    /**
     * Get the details of the available time series
     * @param timeSeriesClient timeSeriesClientObject
     * @return Vector<TimeSeriesInfo>
     */
    /*
        Method is synchronized so the list of time series is only initialised once
     */
    static synchronized Vector<TimeSeriesInfo> getTimeSeriesDetails(TimeSeriesClient timeSeriesClient){
        if(timeSeriesInfoList == null) {
            Vector<String> timeSeriesNames = Utilities.getTimeSeriesNames(timeSeriesClient);
            timeSeriesInfoList = new Vector<>();

            for (String timeSeriesName : timeSeriesNames)
                timeSeriesInfoList.add(TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient, timeSeriesName));
        }
        return timeSeriesInfoList;
    }

    /**
     * Utility function summarising the queries that will be run
     * @param timeSeriesInfoVector Vector of TimeSeriesInfo objects
     * @return String giving series count and points per query
     */
    static String timeSeriesInfoSummary(Vector<TimeSeriesInfo> timeSeriesInfoVector){
        int seriesCount = timeSeriesInfoVector.size();
        long avgDataPointCount = 0;
        for(TimeSeriesInfo timeSeriesInfo : timeSeriesInfoVector) avgDataPointCount+= timeSeriesInfo.getDataPointCount();
        avgDataPointCount /= seriesCount;
        return String.format("Time series count : %d, Average data point count per query %d",seriesCount,avgDataPointCount);
    }
}
