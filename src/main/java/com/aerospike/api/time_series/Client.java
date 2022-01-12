package com.aerospike.api.time_series;

import com.aerospike.client.*;
import com.aerospike.client.cdt.*;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;

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
        Saves data point to the database

        By default insert always goes to the current block for the time series which has key TimeSeriesName

        If Max Values per block is exceeded, save block under name TimeSeries-StartTime and remove 'current' block
     */
    public void put(String timeSeriesName,DataPoint dataPoint, int maxEntryCount){
        // Rely on automatic map creation - don't need to explicitly create a map - put will do that for you
        // createOnlyMapPolicy ensures we are not over-writing the start time for the block
        Record r = asClient.operate(Constants.DEFAULT_WRITE_POLICY,keyForTimeSeries(timeSeriesName),
                // Inserts data point
                MapOperation.put(insertMapPolicy,Constants.TIME_SERIES_BIN_NAME,
                        new Value.LongValue(dataPoint.getTimestamp()),new Value.DoubleValue(dataPoint.getValue())),
                // Store time series name at time of creation
                MapOperation.put(createOnlyMapPolicy,Constants.METADATA_BIN_NAME,new Value.StringValue(Constants.TIME_SERIES_NAME_FIELD_NAME),new Value.StringValue(timeSeriesName)),
                // Start time for block
                MapOperation.put(createOnlyMapPolicy,Constants.METADATA_BIN_NAME,new Value.StringValue(Constants.START_TIME_FIELD_NAME),new Value.LongValue(dataPoint.getTimestamp())),
                // Max entries for block
                MapOperation.put(createOnlyMapPolicy,Constants.METADATA_BIN_NAME,new Value.StringValue(Constants.MAX_BLOCK_TIME_SERIES_ENTRIES_FIELD_NAME),new Value.LongValue(maxEntryCount))
        );
        // Put operation returns map size by default
        long mapSize = r.getLong(Constants.TIME_SERIES_BIN_NAME);
        // If it is greater than the required size, save a copy of the block with key TimeSeries-StartTime
        if(mapSize > maxEntryCount){
            Record r2 = asClient.get(null,keyForTimeSeries(timeSeriesName));
            Bin[] bins = new Bin[2];
            bins[0] = new Bin(Constants.TIME_SERIES_BIN_NAME,r2.getValue(Constants.TIME_SERIES_BIN_NAME));
            bins[1] = new Bin(Constants.METADATA_BIN_NAME,r2.getValue(Constants.METADATA_BIN_NAME));
            long startTime = (Long)r2.getMap(Constants.METADATA_BIN_NAME).get(Constants.START_TIME_FIELD_NAME);
            String newTimeSeriesName = String.format("%s-%d",timeSeriesName,startTime);
            asClient.put(Constants.DEFAULT_WRITE_POLICY,keyForTimeSeries(newTimeSeriesName),bins);
            // and remove the current block
            if(asClient.exists(null,keyForTimeSeries(newTimeSeriesName))) asClient.delete(Constants.DEFAULT_WRITE_POLICY,keyForTimeSeries(timeSeriesName));
        }
    }

    public void put(String timeSeriesName,DataPoint dataPoint){
        put(timeSeriesName, dataPoint,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
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
