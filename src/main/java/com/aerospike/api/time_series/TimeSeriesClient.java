package com.aerospike.api.time_series;

import com.aerospike.client.*;
import com.aerospike.client.cdt.*;

import java.util.*;

public class TimeSeriesClient implements ITimeSeriesClient {
    // Aerospike Client required
    AerospikeClient asClient;
    // Define namespace used as part of initialisation
    String asNamespace;

    // Map policy for inserts
    MapPolicy insertMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
    MapPolicy createOnlyMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY + MapWriteFlags.NO_FAIL);

    // We need a special way of referring to the current record block for a time series - use CURRENT_RECORD_TIMESTAMP
    static final long CURRENT_RECORD_TIMESTAMP = 0;

    public TimeSeriesClient(String asHostName, String asNamespace) {
        asClient = new AerospikeClient(asHostName, Constants.DEFAULT_AEROSPIKE_PORT);
        this.asNamespace = asNamespace;
    }

    /**
     * Saves data point to the database
     * <p>
     * By default insert always goes to the current block for the time series which has key TimeSeriesName
     * <p>
     * If Max Values per block is exceeded, save block under name TimeSeries-StartTime and remove 'current' block
     *
     * @param timeSeriesName
     * @param dataPoint
     * @param maxEntryCount
     */
    public void put(String timeSeriesName, DataPoint dataPoint, int maxEntryCount) {
        // Rely on automatic map creation - don't need to explicitly create a map - put will do that for you
        // createOnlyMapPolicy ensures we are not over-writing the start time for the block
        Record r = asClient.operate(Constants.DEFAULT_WRITE_POLICY, asCurrentKeyForTimeSeries(timeSeriesName),
                // Inserts data point
                MapOperation.put(insertMapPolicy, Constants.TIME_SERIES_BIN_NAME,
                        new Value.LongValue(dataPoint.getTimestamp()), new Value.DoubleValue(dataPoint.getValue())),
                // Store time series name at time of creation
                MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.TIME_SERIES_NAME_FIELD_NAME), new Value.StringValue(timeSeriesName)),
                // Start time for block
                MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.START_TIME_FIELD_NAME), new Value.LongValue(dataPoint.getTimestamp())),
                // Max entries for block
                MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.MAX_BLOCK_TIME_SERIES_ENTRIES_FIELD_NAME), new Value.LongValue(maxEntryCount))
        );
        // Put operation returns map size by default
        long mapSize = r.getLong(Constants.TIME_SERIES_BIN_NAME);
        // If it is greater than the required size, save a copy of the block with key TimeSeries-StartTime
        if (mapSize >= maxEntryCount) {
            // Copy of record, with key timeSeriesName-StartTime
            Record r2 = asClient.get(null, asCurrentKeyForTimeSeries(timeSeriesName));
            Bin[] bins = new Bin[2];
            bins[0] = new Bin(Constants.TIME_SERIES_BIN_NAME, r2.getValue(Constants.TIME_SERIES_BIN_NAME));
            bins[1] = new Bin(Constants.METADATA_BIN_NAME, r2.getValue(Constants.METADATA_BIN_NAME));
            long startTime = (Long) r2.getMap(Constants.METADATA_BIN_NAME).get(Constants.START_TIME_FIELD_NAME);
            addTimeSeriesIndexRecord(timeSeriesName,startTime);
            asClient.put(Constants.DEFAULT_WRITE_POLICY, asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTime), bins);
            // Make the time series a key ordered map
            asClient.operate(Constants.DEFAULT_WRITE_POLICY,asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTime),
                    MapOperation.setMapPolicy(new MapPolicy(MapOrder.KEY_ORDERED,MapWriteMode.UPDATE),Constants.TIME_SERIES_BIN_NAME));
            // and remove the current block
            if (asClient.exists(null, asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTime)))
                asClient.delete(Constants.DEFAULT_WRITE_POLICY, asCurrentKeyForTimeSeries(timeSeriesName));
        }
    }

    /**
     * Saves data point to the database
     * <p>
     * By default insert always goes to the current block for the time series which has key TimeSeriesName
     * <p>
     * If *default* max values per block is exceeded, save block under name TimeSeries-StartTime and remove 'current' block
     *
     * @param timeSeriesName
     * @param dataPoint
     */
    public void put(String timeSeriesName, DataPoint dataPoint) {
        put(timeSeriesName, dataPoint, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
    }

//    /**
//     * Internal method - retrieve time series data points with start and end time expressed
//     * as unix epochs (seconds since 1st Jan 1970) multiplied by required resolution (10^Constants.TIMESTAMP_DECIMAL_PLACES_PER_SECOND)
//     *
//     * @param timeSeriesName
//     * @param startTime
//     * @param endTime
//     * @return
//     */
//    private DataPoint[] getPoints2(String timeSeriesName, long startTime, long endTime) {
//        Record r = asClient.operate(Constants.DEFAULT_WRITE_POLICY, asCurrentKeyForTimeSeries(timeSeriesName),
//                MapOperation.getByKeyRange(Constants.TIME_SERIES_BIN_NAME, new Value.LongValue(startTime), new Value.LongValue(endTime + 1), MapReturnType.KEY_VALUE));
//        if (r != null) {
//            List<Map.Entry<Long, Double>> list = (List<Map.Entry<Long, Double>>) r.getList(Constants.TIME_SERIES_BIN_NAME);
//            DataPoint[] dataPointArray = new DataPoint[list.size()];
//            for (int i = 0; i < list.size(); i++) {
//                dataPointArray[i] = new DataPoint(list.get(i).getKey(), list.get(i).getValue());
//            }
//            return dataPointArray;
//        } else {
//            return new DataPoint[0];
//        }
//    }



    /**
     * Retrieve a specific data point for a named time series
     *
     * @param timeSeriesName
     * @param dateTime
     * @return
     */
    public DataPoint getPoint(String timeSeriesName, Date dateTime) {
        DataPoint[] dataPointArray = getPoints(timeSeriesName, dateTime, dateTime);
        if (dataPointArray.length == 1) {
            return dataPointArray[0];
        } else {
            return null;
        }
    }

    /**
     * Retrieve all time series points between two given date / times (inclusive)
     *
     * @param timeSeriesName
     * @param fromDateTime
     * @param toDateTime
     * @return Datapoints - array of DataPoint objects
     */
    public DataPoint[] getPoints(String timeSeriesName, Date fromDateTime, Date toDateTime) {
        return getPoints(timeSeriesName, DataPoint.epochSecondsToTimestamp(fromDateTime.getTime()), DataPoint.epochSecondsToTimestamp(toDateTime.getTime()));
    }

    /**
     * Aerospike Key for a given time series name
     *
     * @param timeSeriesName
     * @return
     */
    private Key asCurrentKeyForTimeSeries(String timeSeriesName) {
        return new Key(asNamespace, Constants.AS_TIME_SERIES_SET, timeSeriesName);
    }

    /**
     * Aerospike Key for a given time series name
     *
     * @param timeSeriesName
     * @return
     */
    private Key asKeyForHistoricTimeSeriesBlock(String timeSeriesName, long timestamp) {
        String historicBlockKey = String.format("%s-%d", timeSeriesName, timestamp);
        return new Key(asNamespace, Constants.AS_TIME_SERIES_SET, historicBlockKey);
    }

    /**
     * Aerospike Index Key name for a given time series name
     *
     * @param timeSeriesName
     * @return
     */
    private Key asKeyForTimeSeriesIndexes(String timeSeriesName) {
        return new Key(asNamespace, Constants.AS_TIME_SERIES_INDEX_SET, timeSeriesName);
    }

    /**
     * Each time series will have a number of Aerospike records associated with it
     * We keep a record of these to make data retrieval efficient
     *
     * @param timeSeriesName
     * @param startTime
     */
    private void addTimeSeriesIndexRecord(String timeSeriesName, long startTime) {
        // Rely on automatic map creation - don't need to explicitly create a map - put will do that for you
        asClient.operate(Constants.DEFAULT_WRITE_POLICY, asKeyForTimeSeriesIndexes(timeSeriesName),
                // Inserts data point
                MapOperation.put(insertMapPolicy, Constants.TIME_SERIES_INDEX_BIN_NAME,
                        new Value.LongValue(startTime), new Value.StringValue(asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTime).userKey.toString()))
        );

    }

    /**
     * Internal method to calculate the start times of the blocks we need to retrieve for time range represented by
     * timestamps startTime and endTime
     * @param timeSeriesName
     * @param startTime
     * @param endTime
     * @return
     */
    /*
        Algorithm is, to find first block, go forward until we find the first start time after startTime then go back one
        and for endTime, go back until we find the first start time that is after the end time

        Doesn't work if endTime / startTime are inverted so require specific logic for that
     */
    long[] getTimestampsForTimeSeries(String timeSeriesName,long startTime,long endTime){
        if(endTime >= startTime) {
            Record indexListRecord = asClient.operate(Constants.DEFAULT_WRITE_POLICY, asKeyForTimeSeriesIndexes(timeSeriesName),
                    MapOperation.getByKeyRange(Constants.TIME_SERIES_INDEX_BIN_NAME, null, null, MapReturnType.KEY));
            if(indexListRecord != null) {
                List<Long> timestampList = (List<Long>) indexListRecord.getList(Constants.TIME_SERIES_INDEX_BIN_NAME);
                int indexOfFirstTimestamp = 0;
                int indexOfLastTimestamp = timestampList.size() - 1;
                while ((indexOfFirstTimestamp <= timestampList.size() - 2) && (timestampList.get(indexOfFirstTimestamp) < startTime))
                    indexOfFirstTimestamp++;
                if (timestampList.get(indexOfFirstTimestamp) > startTime)
                    indexOfFirstTimestamp = Math.max(0, indexOfFirstTimestamp - 1);
                while (timestampList.get(indexOfLastTimestamp) > endTime) indexOfLastTimestamp--;
                // If we are bringing back the most recent block available we might need the current block - need a special way of indicating this
                int extraTimestampSlot = indexOfLastTimestamp == timestampList.size() - 1 ? 1 : 0;
                long[] timestamps = new long[indexOfLastTimestamp - indexOfFirstTimestamp + 1 + extraTimestampSlot];
                for (int i = 0; i < timestamps.length - extraTimestampSlot; i++) {
                    timestamps[i] = timestampList.get(i + indexOfFirstTimestamp);
                }
                if (extraTimestampSlot == 1) timestamps[timestamps.length - 1] = CURRENT_RECORD_TIMESTAMP;
                return timestamps;
            }
            // If there's no index records, could be in the current block
            else
                return new long[]{CURRENT_RECORD_TIMESTAMP};
        }
        else
            return new long[0];
    }

    Key[] getKeysForQuery(String timeSeriesName, long startTime, long endTime) {
        long[] startTimesForBlocks = getTimestampsForTimeSeries(timeSeriesName, startTime, endTime);
        Key[] keysForQuery = new Key[startTimesForBlocks.length];
        for (int i = 0; i < startTimesForBlocks.length - 1; i++)
            keysForQuery[i] = asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTimesForBlocks[i]);
        if ((startTimesForBlocks.length > 0)) {
            if (startTimesForBlocks[startTimesForBlocks.length - 1] == CURRENT_RECORD_TIMESTAMP) {
                keysForQuery[startTimesForBlocks.length - 1] = asCurrentKeyForTimeSeries(timeSeriesName);
            } else {
                keysForQuery[startTimesForBlocks.length - 1] = asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTimesForBlocks[startTimesForBlocks.length - 1]);
            }
        }
        return keysForQuery;
    }
    /**
     * Internal method - retrieve time series data points with start and end time expressed
     * as unix epochs (seconds since 1st Jan 1970) multiplied by required resolution (10^Constants.TIMESTAMP_DECIMAL_PLACES_PER_SECOND)
     *
     * @param timeSeriesName
     * @param startTime
     * @param endTime
     * @return
     */
    private DataPoint[] getPoints(String timeSeriesName, long startTime, long endTime) {
        Key[] keys = getKeysForQuery(timeSeriesName,startTime,endTime);
        Record[] timeSeriesBlocks = asClient.get(null,keys,Constants.TIME_SERIES_BIN_NAME);
        int recordCount = 0;
        for(int i=0;i< timeSeriesBlocks.length;i++){
            Record currentRecord = timeSeriesBlocks[i];
            // Null record is a possibility if we have just made the current block a historic block
            if(currentRecord != null) {
                Map<Long, Double> timeSeries = (Map<Long, Double>) currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME);
                Iterator<Long> timestamps = timeSeries.keySet().iterator();
                while (timestamps.hasNext()) {
                    long timestamp = timestamps.next();
                    if (timestamp >= startTime && timestamp <= endTime) recordCount++;
                }
            }
        }

        DataPoint[] dataPoints = new DataPoint[recordCount];
        int index = 0;

        for(int i=0;i< timeSeriesBlocks.length;i++) {
            // Null record is a possibility if we have just made the current block a historic block
            Record currentRecord = timeSeriesBlocks[i];
            if(currentRecord != null) {
                Map<Long, Double> timeSeries = (Map<Long, Double>) currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME);
                List<Long> sortedList = new ArrayList<>(timeSeries.keySet());
                Collections.sort(sortedList);
                for (int j = 0; j < sortedList.size(); j++) {
                    long timestamp = sortedList.get(j);
                    if ((startTime <= timestamp) && (timestamp <= endTime)) {
                        dataPoints[index] = new DataPoint(timestamp, timeSeries.get(timestamp));
                        index++;
                    }
                }
            }
        }
        return dataPoints;
    }

}
