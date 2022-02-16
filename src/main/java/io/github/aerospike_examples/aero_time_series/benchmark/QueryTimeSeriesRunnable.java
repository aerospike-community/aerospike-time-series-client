package io.github.aerospike_examples.aero_time_series.benchmark;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesInfo;

import java.util.Date;
import java.util.Vector;

class QueryTimeSeriesRunnable extends TimeSeriesRunnable{
    private final TimeSeriesClient timeSeriesClient;
    private final long runDurationSeconds;

    public QueryTimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        super(asClient, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, randomSeed);
        timeSeriesClient = new TimeSeriesClient(asClient,asNamespace,asSet,timeSeriesCountPerObject);
        runDurationSeconds = benchmarkClient.runDuration;
    }

    public void run(){
        startTime = System.currentTimeMillis();
        Vector<TimeSeriesInfo> timeSeriesInfoList = getTimeSeriesDetails(timeSeriesClient);

        isRunning = true;
        while(System.currentTimeMillis() - startTime < runDurationSeconds * Constants.MILLISECONDS_IN_SECOND){
            TimeSeriesInfo timeSeriesInfo = timeSeriesInfoList.get(random.nextInt(timeSeriesInfoList.size()));
            timeSeriesClient.runQuery(timeSeriesInfo.getSeriesName(),TimeSeriesClient.QueryOperation.AVG,
                    new Date(timeSeriesInfo.getStartDateTime()),new Date(timeSeriesInfo.getEndDateTime()));
            updateCount++;
        }
        isFinished = true;
        isRunning = false;

    }

    /**
     * Get the details of the available time series
     * @param timeSeriesClient timeSeriesClientObject
     * @return Vector<TimeSeriesInfo>
     */
    static Vector<TimeSeriesInfo> getTimeSeriesDetails(TimeSeriesClient timeSeriesClient){
        Vector<String> timeSeriesNames = Utilities.getTimeSeriesNames(timeSeriesClient);
        Vector<TimeSeriesInfo> timeSeriesInfoList = new Vector<>();

        for(String timeSeriesName : timeSeriesNames) timeSeriesInfoList.add(TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient, timeSeriesName));

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
