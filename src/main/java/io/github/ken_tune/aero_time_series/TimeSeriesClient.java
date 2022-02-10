package io.github.ken_tune.aero_time_series;

import com.aerospike.client.*;
import com.aerospike.client.cdt.*;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

import java.util.*;
import java.util.stream.Collectors;

public class TimeSeriesClient implements ITimeSeriesClient {
    /*
     The code for 'archiving' blocks of data means no data will ever be lost - the current block will only be deleted after
     1) it has been copied
     2) the original bleock has not changed in the meantime
     If we detect 2) ( & note that 1) must have occurred before we get to check 2) )
     Then we retry a set number of times - governed by the parameter below
     Note that
     If we don't succeed in RETRY_COUNT_FOR_FAILED_BLOCK_COPY times, we still have all the data, but we may have it twice
     Eventually when the code runs all the way through, normal operation will be restored
     In the meantime, we have duplicate resolution built into the 'get' calls
    */
    public static final int RETRY_COUNT_FOR_FAILED_BLOCK_COPY = 5;

    // Aerospike Client required
    AerospikeClient asClient;
    // Define namespace used as part of initialisation
    String asNamespace;

    // Set for time series
    private String timeSeriesSet = Constants.DEFAULT_TIME_SERIES_SET;

    // Read and write policies
    private Policy readPolicy = new Policy();
    private WritePolicy writePolicy = new WritePolicy();

    // Max entry count per data block
    int maxBlockEntryCount = Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK;


    // Map policy for inserts - these are not modifiable
    private final MapPolicy insertMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
    private final MapPolicy createOnlyMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY + MapWriteFlags.NO_FAIL);

    // We need a special way of referring to the current record block for a time series - use CURRENT_RECORD_TIMESTAMP
    static final long CURRENT_RECORD_TIMESTAMP = 0;
    public final static String TIME_SERIES_INDEX_SET_SUFFIX = "idx";

    // Package level visible paramaters allowing testing of correct handling of race conditions
    boolean testMode = false;
    double failurePctRateForCopyBlock =0;

    /**
     * TimeSeriesClient constructor. Provide an Aerospike Client object, tne namespace, the name of the set to use, max number of data points per Aerospike object
     * @param asClient
     * @param asNamespace
     * @param timeSeriesSet
     * @param maxBlockEntryCount
     */
    public TimeSeriesClient(AerospikeClient asClient, String asNamespace, String timeSeriesSet, int maxBlockEntryCount) {
        this.asClient = asClient;
        this.asNamespace = asNamespace;
        this.maxBlockEntryCount = maxBlockEntryCount;
        this.timeSeriesSet = timeSeriesSet;
    }

    /**
     * TimeSeriesClient constructor. Provide an Aerospike Client object and tne namespace
     * Max data points per object gets set to the default value.
     * @param asClient
     * @param asNamespace
     */
    public TimeSeriesClient(AerospikeClient asClient, String asNamespace) {
        this(asClient,asNamespace,Constants.DEFAULT_TIME_SERIES_SET, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
    }

    /*
        Getters / setters for policy objets
     */
    /**
     * Read policy used for Aerospike transactions. See https://docs.aerospike.com/guide/policies for more details
     * @return
     */
    public Policy getReadPolicy() {
        return readPolicy;
    }

    /**
     * Setter method for read policy object
     * @param readPolicy
     */
    public void setReadPolicy(Policy readPolicy) {
        this.readPolicy = readPolicy;
    }

    /**
     * Write policy used for Aerospike transactions. See https://docs.aerospike.com/guide/policies for more details
     * @return
     */
    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    /**
     * Setter method for write policy object
     * @param writePolicy
     */
    public void setWritePolicy(WritePolicy writePolicy) {
        this.writePolicy = writePolicy;
    }

    /**
     * Setter method for time series set
     * @return
     */
    public String getTimeSeriesSet() {
        return timeSeriesSet;
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
     */
    public void put(String timeSeriesName, DataPoint dataPoint) {
        // Rely on automatic map creation - don't need to explicitly create a map - put will do that for you
        // Need to put the metadata ops and the insert together in one array
        Operation[] ops = new Operation[4];
        // Data point put operation
        ops[0] = MapOperation.put(insertMapPolicy, Constants.TIME_SERIES_BIN_NAME, new Value.LongValue(dataPoint.getTimestamp()), new Value.DoubleValue(dataPoint.getValue()));
        // Metadata operations
        Operation[] metadataOps = opsForMetadataCreation(timeSeriesName,dataPoint.getTimestamp(), maxBlockEntryCount);
        // Add to the actual operations list
        ops[1] = metadataOps[0]; ops[2]=metadataOps[1]; ops[3]=metadataOps[2];
        // and execute
        Record r = asClient.operate(writePolicy, asCurrentKeyForTimeSeries(timeSeriesName),ops);
        // Put operation returns map size by default
        long mapSize = r.getLong(Constants.TIME_SERIES_BIN_NAME);
        // If it is greater than the required size, save a copy of the block with key TimeSeries-StartTime
        if (mapSize >= maxBlockEntryCount) {
            copyCurrentDataToHistoricBlock(timeSeriesName);
        }
    }

    /**
     * Private method to create the operations needed to insert the metadata for a time series block
     * Breaking out as a separate method as we use in more than one place
     * @param timeSeriesName
     * @param timestamp
     * @param maxEntryCount
     * @return
     */
    private Operation[] opsForMetadataCreation(String timeSeriesName,long timestamp,long maxEntryCount){
        // createOnlyMapPolicy ensures we are not over-writing the start time for the block
        Operation[] opsForMetadataCreation = new Operation[3];
        // Store time series name at time of creation
        opsForMetadataCreation[0] =
                MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.TIME_SERIES_NAME_FIELD_NAME), new Value.StringValue(timeSeriesName));
        // Start time for block
        opsForMetadataCreation[1] =
            MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.START_TIME_FIELD_NAME), new Value.LongValue(timestamp));
        // Max entries for block
        opsForMetadataCreation[2] =
            MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.MAX_BLOCK_TIME_SERIES_ENTRIES_FIELD_NAME), new Value.LongValue(maxEntryCount));
        return opsForMetadataCreation;
    }
    /**
     * Take current block for timeSeriesName and copy it to a historic block
     * Remove the current block when done
     *
     * @param timeSeriesName
     */
    private void copyCurrentDataToHistoricBlock(String timeSeriesName) {
        copyCurrentDataToHistoricBlock(timeSeriesName,RETRY_COUNT_FOR_FAILED_BLOCK_COPY);
    }

    /**
     * copyCurrentDataToHistoricBlock(String timeSeriesName) will retry a set number of times if there is a failure
     * This is facilitated via the function below. Every time we retry, retryCount is deprecated
     *
     * @param timeSeriesName
     * @param retryCount
     */
    private void copyCurrentDataToHistoricBlock(String timeSeriesName, int retryCount){
        Record currentRecord = asClient.get(readPolicy, asCurrentKeyForTimeSeries(timeSeriesName));
        Bin[] bins = new Bin[2];
        // Copying of time series bin requires slightly convoluted approach below
        bins[0] = new Bin(Constants.TIME_SERIES_BIN_NAME,
                ((Map<Long,Double>)currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME)).entrySet().stream().collect(Collectors.toList()),
                MapOrder.KEY_ORDERED);
        // Now set up the metadata - add in the timestamp of the most recent observation
        Map metadata = currentRecord.getMap(Constants.METADATA_BIN_NAME);
        long lastTimestamp = Collections.max(((Map<Long,Double>)currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME)).keySet());
        metadata.put(Constants.END_TIME_FIELD_NAME,lastTimestamp);
        bins[1] = new Bin(Constants.METADATA_BIN_NAME,metadata);

        long startTime = (Long) currentRecord.getMap(Constants.METADATA_BIN_NAME).get(Constants.START_TIME_FIELD_NAME);

        addTimeSeriesIndexRecord(timeSeriesName,startTime);
        asClient.put(writePolicy, asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTime), bins);
        // and remove the current block, if the archived block exists
        // Strictly speaking I don't think the 'exists' check is necessary, but it does make things clear
        if (asClient.exists(readPolicy, asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTime))) {
            // We check that in the meantime the current record has not changed via the generation check
            WritePolicy checkGenerationWritePolicy = new WritePolicy(writePolicy);
            checkGenerationWritePolicy.generation = currentRecord.generation;
            checkGenerationWritePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            // This code is for testing purposes to verify that even if the current record is modified
            // we still get correct results
            // testMode = true should only be set by test code
            if(testMode) {
                // & we mimic 'new writes' with probability failurePctRateForCopyBlock, set to zero by default
                if (new Random().nextDouble() < failurePctRateForCopyBlock / 100) {
                    asClient.touch(writePolicy, asCurrentKeyForTimeSeries(timeSeriesName));
                    System.out.println("Failure triggered in copy block section");
                }
            }
            // Out of test code block. If the delete fails, we retry
            try {
                asClient.delete(checkGenerationWritePolicy, asCurrentKeyForTimeSeries(timeSeriesName));
            }
            catch(AerospikeException e){
                if(e.getResultCode() == ResultCode.GENERATION_ERROR){
                    if(retryCount > 0){
                        retryCount--;
                        copyCurrentDataToHistoricBlock(timeSeriesName, retryCount);
                    }
                }
            }
        }
    }

    /**
     * Save data points to the database
     * <p>
     * Inserts always go to the current block for the time series which has key TimeSeriesName
     *
     * @param timeSeriesName - time series name
     * @param dataPoints - data points as an array
     */
    public void put(String timeSeriesName, DataPoint[] dataPoints) {
        // First of all need to find out how much 'room' is available
        Record r = asClient.operate(writePolicy,asCurrentKeyForTimeSeries(timeSeriesName),MapOperation.size(Constants.TIME_SERIES_BIN_NAME));
        int existingRecordCount = 0;
        if(r != null) existingRecordCount = r.getInt(Constants.TIME_SERIES_BIN_NAME);

        // We will be working through the data points iteratively, so we need to keep track of where we are
        int lastRecordLoaded = 0;

        // Stop when all records have been 'put'
        while(lastRecordLoaded < dataPoints.length){
            // Load records remaining or whatever we have space for, whichever is the smaller
            int numberOfRecordsToLoad = Math.min(dataPoints.length - lastRecordLoaded,maxBlockEntryCount - existingRecordCount);
            // Insert metadata - may not be needed, but will be ignored if it already exists
            Operation[] metadataOps = opsForMetadataCreation(timeSeriesName,dataPoints[lastRecordLoaded].getTimestamp(),maxBlockEntryCount);
            // Batch inserting datapoints is done via an array of operations
            Operation[] ops  = new Operation[numberOfRecordsToLoad + metadataOps.length];
            // Construct the array of operations
            for(int i=lastRecordLoaded;i<lastRecordLoaded+numberOfRecordsToLoad;i++) ops[i - lastRecordLoaded] = MapOperation.put(insertMapPolicy,Constants.TIME_SERIES_BIN_NAME,
                    Value.get(dataPoints[i].getTimestamp()),Value.get(dataPoints[i].getValue()));
            // and add the metadata
            for(int i=0;i<metadataOps.length;i++) ops[numberOfRecordsToLoad + i] = metadataOps[i];
            // Put to the database
            asClient.operate(writePolicy,asCurrentKeyForTimeSeries(timeSeriesName),ops);
            // If the block is full, 'archive' it
            if(numberOfRecordsToLoad  + existingRecordCount == maxBlockEntryCount) copyCurrentDataToHistoricBlock(timeSeriesName);
            // If we're at this point in the code we know we'll be inserting to an empty block
            existingRecordCount = 0;
            // Update the running total of records we've inserted
            lastRecordLoaded += numberOfRecordsToLoad;
        }

    }

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
        return getPoints(timeSeriesName, fromDateTime.getTime(), toDateTime.getTime());
    }

    /**
     * Aerospike Key for a given time series name
     * Package level visibility to allow testing
     *
     * @param timeSeriesName
     * @return
     */
    Key asCurrentKeyForTimeSeries(String timeSeriesName) {
        return new Key(asNamespace, timeSeriesSet, timeSeriesName);
    }

    /**
     * Aerospike Key for a given time series name
     *
     * @param timeSeriesName
     * @return
     */
    private Key asKeyForHistoricTimeSeriesBlock(String timeSeriesName, long timestamp) {
        String historicBlockKey = String.format("%s-%d", timeSeriesName, timestamp);
        return new Key(asNamespace, timeSeriesSet, historicBlockKey);
    }

    /**
     * Aerospike Index Key name for a given time series name
     *
     * @param timeSeriesName
     * @return
     */
    private Key asKeyForTimeSeriesIndexes(String timeSeriesName) {
        return new Key(asNamespace, timeSeriesIndexSetName(), timeSeriesName);
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
        asClient.operate(writePolicy, asKeyForTimeSeriesIndexes(timeSeriesName),
                Operation.put(new Bin(Constants.TIME_SERIES_NAME_FIELD_NAME,new Value.StringValue(timeSeriesName))),
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

        Doesn't work if endTime / startTime are i   nverted so require specific logic for that
     */
    long[] getTimestampsForTimeSeries(String timeSeriesName,long startTime,long endTime){
        if(endTime >= startTime) {
            Record indexListRecord = asClient.operate(writePolicy, asKeyForTimeSeriesIndexes(timeSeriesName),
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
        Record[] timeSeriesBlocks = asClient.get(new BatchPolicy(readPolicy),keys,Constants.TIME_SERIES_BIN_NAME);
        // uniqueTimestampMap is used to count the unique timestamps
        Map<Long,Integer> uniqueTimestampMap = new HashMap<>();
        // First we need to count timestamps
        for(int i=0;i< timeSeriesBlocks.length;i++){
            Record currentRecord = timeSeriesBlocks[i];
            // Null record is a possibility if we have just made the current block a historic block
            if(currentRecord != null) {
                Map<Long, Double> timeSeries = (Map<Long, Double>) currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME);
                Iterator<Long> timestamps = timeSeries.keySet().iterator();
                while (timestamps.hasNext()) {
                    long timestamp = timestamps.next();
                    if (timestamp >= startTime && timestamp <= endTime)
                        if(uniqueTimestampMap.get(timestamp) == null) {
                            uniqueTimestampMap.put(timestamp,1);
                        }
                }
            }
        }
        // Then initialise an appropriately sized array
        DataPoint[] dataPoints = new DataPoint[uniqueTimestampMap.size()];

        // Make use of uniqueTimestampMap to make sure we get rid of any duplicate points
        // These can arise if copyCurrentDataToHistoricBlock does not run to completion
        // Note the duplicates will eventually be naturally eliminated

        // index is used to track where we are when adding to the returned array
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
                        // Only add one point per timestamp to the array returned
                        // Check below makes sure this happens
                        if(uniqueTimestampMap.get(timestamp) != null) {
                            dataPoints[index] = new DataPoint(timestamp, timeSeries.get(timestamp));
                            uniqueTimestampMap.remove(timestamp);
                            index++;
                        }
                    }
                }
            }
        }
        return dataPoints;
    }

    /**
     * We store some 'indexes' to make the time series db work.
     * This gives the name of the set they're stored in
     * It simply suffixes the time series set name with TIME_SERIES_INDEX_SET_SUFFIX
     * @return
     */
    public String timeSeriesIndexSetName(){
        return timeSeriesIndexSetName(timeSeriesSet);
    }

    /**
     * Static method allowing inference of the time index set name - useful in a couple of places elsewhere
     * @param setName
     * @return
     */
    public static String timeSeriesIndexSetName(String setName){
        return String.format("%s%s",setName,TIME_SERIES_INDEX_SET_SUFFIX);
    }
}
