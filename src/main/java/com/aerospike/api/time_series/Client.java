package com.aerospike.api.time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.*;

import java.util.*;

public class Client implements IClient {
    AerospikeClient asClient;
    String asNamespace;

    // Map policy for inserts
    MapPolicy insertMapPolicy =  new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
    MapPolicy createOnlyMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED,MapWriteFlags.CREATE_ONLY + MapWriteFlags.NO_FAIL);


    protected Client(String asHostName,String asNamespace){
            asClient = new AerospikeClient(asHostName,Constants.DEFAULT_AEROSPIKE_PORT);
            this.asNamespace = asNamespace;
    }

    /*
        Make this a key ordered map
     */
    public void put(String timeSeriesName,DataPoint dataPoint){
        // Rely on automatic map creation - don't need to explicitly create a map - put will do that for you
        Record r = asClient.operate(Constants.DEFAULT_WRITE_POLICY,keyForTimeSeries(timeSeriesName),
                MapOperation.put(insertMapPolicy,Constants.TIME_SERIES_BIN_NAME,
                        new Value.LongValue(dataPoint.getTimestamp()),new Value.DoubleValue(dataPoint.getValue())),
                MapOperation.put(createOnlyMapPolicy,"metadata",new Value.StringValue("TimeSeriesName"),new Value.StringValue(timeSeriesName)),
                MapOperation.put(createOnlyMapPolicy,"metadata",new Value.StringValue("StartTime"),new Value.LongValue(dataPoint.getTimestamp()))
        );
        // Put operation returns map size by default
        long mapSize = r.getLong(Constants.TIME_SERIES_BIN_NAME);
    }

    private DataPoint[] getPoints(String timeSeriesName,long startTime, long endTime){
        Record r = asClient.operate(Constants.DEFAULT_WRITE_POLICY,keyForTimeSeries(timeSeriesName),
                MapOperation.getByKeyRange(Constants.TIME_SERIES_BIN_NAME,new Value.LongValue(startTime),new Value.LongValue(endTime + 1),MapReturnType.KEY_VALUE));
        if(r != null) {
            List<Map.Entry<Long, Double>> list = (List<Map.Entry<Long, Double>>) r.getList(Constants.TIME_SERIES_BIN_NAME);
            DataPoint[] dataPointArray = new DataPoint[list.size()];
            for (int i = 0; i < list.size(); i++) {
                dataPointArray[i] = new DataPoint(list.get(i).getKey(), list.get(i).getValue());
            }
            return dataPointArray;
        }
        else{
            return new DataPoint[0];
        }
    };

    public DataPoint getPoint(String timeSeriesName,Date dateTime){
        DataPoint[] dataPointArray = getPoints(timeSeriesName,dateTime,dateTime);
        if(dataPointArray.length == 1){
            return dataPointArray[0];
        }
        else{
            return null;
        }
    }

    public DataPoint[] getPoints(String timeSeriesName,Date fromDateTime, Date toDateTime){
        return getPoints(timeSeriesName, DataPoint.epochSecondsToTimestamp(fromDateTime.getTime()), DataPoint.epochSecondsToTimestamp(toDateTime.getTime()));
    }

    private Key keyForTimeSeries(String timeSeriesName){
        return new Key(asNamespace,Constants.AS_TIME_SERIES_SET,timeSeriesName);
    }
}
