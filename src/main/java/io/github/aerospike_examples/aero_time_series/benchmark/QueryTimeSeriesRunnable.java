package io.github.aerospike_examples.aero_time_series.benchmark;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesInfo;

import java.util.Vector;

public class QueryTimeSeriesRunnable extends TimeSeriesRunnable{
    TimeSeriesClient timeSeriesClient;
    public QueryTimeSeriesRunnable(AerospikeClient asClient, String asNamespace, String asSet, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        super(asClient, asNamespace, asSet, timeSeriesCountPerObject, benchmarkClient, randomSeed);
        timeSeriesClient = new TimeSeriesClient(asClient,asNamespace,asSet,timeSeriesCountPerObject);
    }

    public void run(){

    }

    static Vector<TimeSeriesInfo> getTimeSeriesDetails(TimeSeriesClient timeSeriesClient){
        Vector<String> timeSeriesNames = Utilities.getTimeSeriesNames(timeSeriesClient);
        Vector<TimeSeriesInfo> timeSeriesInfo = new Vector<>();
        for(String timeSeriesName: timeSeriesNames){
            timeSeriesInfo.add(TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient,timeSeriesName));
        }
        return timeSeriesInfo;
    }
}
