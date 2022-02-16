package io.github.aerospike_examples.aero_time_series.client;

import com.aerospike.client.*;
import com.aerospike.client.cdt.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.*;
import io.github.aerospike_examples.aero_time_series.Constants;

import java.util.*;

public class TimeSeriesClient implements ITimeSeriesClient {
    /**
     * Enumeration of the types of queries we can run against a time series
     */
    public enum QueryOperation
    {
        MAX("max","maximum value of series"),
        MIN("min", "minimum value of series"),
        AVG("avg", "average value of series"),
        COUNT("count","number of values in the series"),
        VOL("vol","volatility of values in series");

        private final String shortName;
        private final String description;

        QueryOperation(String shortName,String description) {
            this.shortName = shortName;
            this.description = description;
        }

        @SuppressWarnings("unused")
        public String getShortName() {
            return shortName;
        }

        @SuppressWarnings("unused")
        public String getDescription(){
            return description;
        }
    }

    /*
     The code for 'archiving' blocks of data means no data will ever be lost - the current block will only be deleted after
     1) it has been copied
     2) the original block has not changed in the meantime
     If we detect 2) ( & note that 1) must have occurred before we get to check 2) )
     Then we retry a set number of times - governed by the parameter below
     Note that
     If we don't succeed in RETRY_COUNT_FOR_FAILED_BLOCK_COPY times, we still have all the data, but we may have it twice
     Eventually when the code runs all the way through, normal operation will be restored
     In the meantime, we have duplicate resolution built into the 'get' calls
    */
    @SuppressWarnings("WeakerAccess")
    public static final int RETRY_COUNT_FOR_FAILED_BLOCK_COPY = 5;

    // Aerospike Client required
    final AerospikeClient asClient;
    // Define namespace used as part of initialisation
    private final String asNamespace;

    // Set for time series
    private String timeSeriesSet = Constants.DEFAULT_TIME_SERIES_SET;

    // Read and write policies
    private Policy readPolicy = new Policy();
    private WritePolicy writePolicy = new WritePolicy();

    // Max entry count per data block
    private int maxBlockEntryCount = Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK;


    // Map policy for inserts - these are not modifiable
    private final MapPolicy insertMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);
    private final MapPolicy createOnlyMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY + MapWriteFlags.NO_FAIL);

    // We need a special way of referring to the current record block for a time series - use CURRENT_RECORD_TIMESTAMP
    static final long CURRENT_RECORD_TIMESTAMP = 0;
    @SuppressWarnings("WeakerAccess")
    public final static String TIME_SERIES_INDEX_SET_SUFFIX = "idx";

    // Package level visible parameters allowing testing of correct handling of race conditions
    boolean testMode = false;
    double failurePctRateForCopyBlock =0;

    /**
     * TimeSeriesClient constructor. Provide an Aerospike Client object, tne namespace, the name of the set to use, max number of data points per Aerospike object
     * @param asClient Aerospike Client
     * @param asNamespace Aerospike namespace
     * @param timeSeriesSet Set to store time series data in
     * @param maxBlockEntryCount max data points per Aerospike object
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
     * @param asClient Aerospike Client
     * @param asNamespace Aerospike namespace
     */
    @SuppressWarnings("unused") // Required in API
    public TimeSeriesClient(AerospikeClient asClient, String asNamespace) {
        this(asClient,asNamespace,Constants.DEFAULT_TIME_SERIES_SET, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
    }

    /*
        Getters / setters for policy objects
     */
    /**
     * Read policy used for Aerospike transactions. See https://docs.aerospike.com/guide/policies for more details
     * @return read policy object
     */
    @SuppressWarnings("unused") // Required in API
    public Policy getReadPolicy() {
        return readPolicy;
    }

    /**
     * Setter method for read policy object
     * @param readPolicy - Aerospike read policy to be used when writing to Aerospike database
     * See https://docs.aerospike.com/guide/policies for more details
     */
    @SuppressWarnings("unused")
    public void setReadPolicy(Policy readPolicy) {
        this.readPolicy = readPolicy;
    }

    /**
     * Write policy used for Aerospike transactions. See https://docs.aerospike.com/guide/policies for more details
     * @return Aerospike Write Policy object
     */
    @SuppressWarnings("unused")
    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    /**
     * Setter method for write policy object
     *
     * @param writePolicy Aerospike write policy to be used when writing to Aerospike database
     * See https://docs.aerospike.com/guide/policies for more details
     */
    @SuppressWarnings("unused")
    public void setWritePolicy(WritePolicy writePolicy) {
        this.writePolicy = writePolicy;
    }

    /**
     * Getter method for time series set
     * @return time series set name
     */
    public String getTimeSeriesSet() {
        return timeSeriesSet;
    }

    /**
     * Getter method for Aerospike Client
     * @return Aerospike Client object
     */
    public AerospikeClient getAsClient(){ return asClient;}

    /**
     * Getter method for Aerospike Namespace
     * @return Aerospike namespace
     */
    public String getAsNamespace(){ return asNamespace;}

    /**
     * Saves data point to the database
     * <p>
     * By default insert always goes to the current block for the time series which has key TimeSeriesName
     * <p>
     * If Max Values per block is exceeded, save block under name TimeSeries-StartTime and remove 'current' block
     *
     * @param timeSeriesName - time series name to write to
     * @param dataPoint - data point to write
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
     * @param timeSeriesName - time series name
     * @param startTimestamp - timestamp
     * @param maxEntryCount - max entry count
     * @return the operations required to build the metadata
     */
    private Operation[] opsForMetadataCreation(String timeSeriesName,long startTimestamp,long maxEntryCount){
        // createOnlyMapPolicy ensures we are not over-writing the start time for the block
        Operation[] opsForMetadataCreation = new Operation[3];
        // Store time series name at time of creation
        opsForMetadataCreation[0] =
                MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.TIME_SERIES_NAME_FIELD_NAME), new Value.StringValue(timeSeriesName));
        // Start time for block
        opsForMetadataCreation[1] =
            MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.START_TIME_FIELD_NAME), new Value.LongValue(startTimestamp));
        // Max entries for block
        opsForMetadataCreation[2] =
            MapOperation.put(createOnlyMapPolicy, Constants.METADATA_BIN_NAME, new Value.StringValue(Constants.MAX_BLOCK_TIME_SERIES_ENTRIES_FIELD_NAME), new Value.LongValue(maxEntryCount));
        return opsForMetadataCreation;
    }
    /**
     * Take current block for timeSeriesName and copy it to a historic block
     * Remove the current block when done
     *
     * @param timeSeriesName - name of series we're processing
     */
    private void copyCurrentDataToHistoricBlock(String timeSeriesName) {
        copyCurrentDataToHistoricBlock(timeSeriesName,RETRY_COUNT_FOR_FAILED_BLOCK_COPY);
    }

    /**
     * copyCurrentDataToHistoricBlock(String timeSeriesName) will retry a set number of times if there is a failure
     * This is facilitated via the function below. Every time we retry, retryCount is deprecated
     *
     * @param timeSeriesName - name of series we're processing
     * @param retryCount - number of retries to allow if there is a generation check error - function calls itself recursively to do this
     */
    private void copyCurrentDataToHistoricBlock(String timeSeriesName, int retryCount){
        Record currentRecord = asClient.get(readPolicy, asCurrentKeyForTimeSeries(timeSeriesName));
        // Need to copy the current record into a historic block
        Bin[] bins = new Bin[2];
        // First the time series bin
        bins[0] = new Bin(Constants.TIME_SERIES_BIN_NAME,
                currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME),
                MapOrder.KEY_ORDERED);
        // Now the metadata - add in the timestamp of the most recent observation
        Map metadata = currentRecord.getMap(Constants.METADATA_BIN_NAME);
        @SuppressWarnings("unchecked") // Should be able to assume the below casting works
        long lastTimestamp = Collections.max(((Map<Long,Double>)currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME)).keySet());
        //noinspection unchecked
        metadata.put(Constants.END_TIME_FIELD_NAME,lastTimestamp);
        bins[1] = new Bin(Constants.METADATA_BIN_NAME,metadata);

        long startTime = (Long) currentRecord.getMap(Constants.METADATA_BIN_NAME).get(Constants.START_TIME_FIELD_NAME);
        long entryCount = currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME).size();

        addTimeSeriesIndexRecord(timeSeriesName,startTime, lastTimestamp,entryCount);
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
            // Batch inserting data points is done via an array of operations
            Operation[] ops  = new Operation[numberOfRecordsToLoad + metadataOps.length];
            // Construct the array of operations
            for(int i=lastRecordLoaded;i<lastRecordLoaded+numberOfRecordsToLoad;i++) ops[i - lastRecordLoaded] = MapOperation.put(insertMapPolicy,Constants.TIME_SERIES_BIN_NAME,
                    Value.get(dataPoints[i].getTimestamp()),Value.get(dataPoints[i].getValue()));
            // and add the metadata
            System.arraycopy(metadataOps, 0, ops, numberOfRecordsToLoad, metadataOps.length);
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
     * @param timeSeriesName name of relevant series
     * @param dateTime timestamp for point we want
     * @return data point if found
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
     * @param timeSeriesName - name of time series
     * @param fromDateTime - start time for required range
     * @param toDateTime - end time for required range
     * @return array of DataPoint objects
     */
    public DataPoint[] getPoints(String timeSeriesName, Date fromDateTime, Date toDateTime) {
        return getPoints(timeSeriesName, fromDateTime.getTime(), toDateTime.getTime());
    }

    /**
     * Aerospike Key for a given time series name
     * Package level visibility to allow testing
     *
     * @param timeSeriesName name of time series
     * @return Aerospike Key for current block for this time series
     */
    Key asCurrentKeyForTimeSeries(String timeSeriesName) {
        return new Key(asNamespace, timeSeriesSet, timeSeriesName);
    }

    /**
     * Aerospike Key for a given block for a given time series
     *
     * @param timeSeriesName - name of time series
     * @param startTimestampForBlock - start timestamp for block
     * @return Aerospike Key for required block
     */
    private Key asKeyForHistoricTimeSeriesBlock(String timeSeriesName, long startTimestampForBlock) {
        String historicBlockKey = String.format("%s-%d", timeSeriesName, startTimestampForBlock);
        return new Key(asNamespace, timeSeriesSet, historicBlockKey);
    }

    /**
     * Aerospike Index Key name for a given time series name
     *
     * @param timeSeriesName - time series in question
     * @return - Aerospike Key to index block for that time series
     */
    private Key asKeyForTimeSeriesIndexes(String timeSeriesName) {
        return new Key(asNamespace, timeSeriesIndexSetName(), timeSeriesName);
    }

    /**
     * Each time series will have a number of Aerospike records associated with it
     * We keep a record of these to make data retrieval efficient
     *
     * @param timeSeriesName - name of time series we are updating index for
     * @param startTime - start time of the block we're adding to the index
     */
    private void addTimeSeriesIndexRecord(String timeSeriesName, long startTime, long endTime, long entryCount) {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put(Constants.END_TIME_FIELD_NAME,endTime);
        metadata.put(Constants.ENTRY_COUNT_FIELD_NAME,entryCount);
        // Rely on automatic map creation - don't need to explicitly create a map - put will do that for you
        asClient.operate(writePolicy, asKeyForTimeSeriesIndexes(timeSeriesName),
                Operation.put(new Bin(Constants.TIME_SERIES_NAME_FIELD_NAME,new Value.StringValue(timeSeriesName))),
                // Inserts data point
                MapOperation.put(insertMapPolicy,Constants.TIME_SERIES_INDEX_BIN_NAME,
                        new Value.LongValue(startTime),new Value.MapValue(metadata))
        );
    }

    /**
     * Internal method to calculate the start times of the blocks we need to retrieve for time range represented by
     * timestamps startTime and endTime
     *
     * @param timeSeriesName - name of time series we are getting block start times for
     * @param startTime - start time of range we're interested in
     * @param endTime - end time of range we're interested in
     * @return long[] containing the timestamps
     */
    /*
        Algorithm is, to find first block, go forward until we find the first start time after startTime then go back one
        and for endTime, go back until we find the first start time that is after the end time

        Doesn't work if endTime / startTime are inverted so require specific logic for that
     */
    long[] getTimestampsForTimeSeries(String timeSeriesName,long startTime,long endTime){
        if(endTime >= startTime) {
            Record indexListRecord = asClient.operate(writePolicy, asKeyForTimeSeriesIndexes(timeSeriesName),
                    MapOperation.getByKeyRange(Constants.TIME_SERIES_INDEX_BIN_NAME, null, null, MapReturnType.KEY));
            if(indexListRecord != null) {
                @SuppressWarnings("unchecked")  // Can assume below casting works
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

    /**
     * Get the Aerospike Keys we need for the data for timeSeriesName between startTime and endTime
     * where this represents milliseconds since the epoch
     *
     * @param timeSeriesName time series name
     * @param startTime start time as long
     * @param endTime end time as long
     * @return Aerospike Key[]
     */
    private Key[] getKeysForQuery(String timeSeriesName, long startTime, long endTime) {
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
     * @param timeSeriesName name of time series we're retrieving points fpr
     * @param startTime start time of required range
     * @param endTime end time of required range
     * @return DataPoint[]
     */
    private DataPoint[] getPoints(String timeSeriesName, long startTime, long endTime) {
        Key[] keys = getKeysForQuery(timeSeriesName,startTime,endTime);
        Record[] timeSeriesBlocks = asClient.get(new BatchPolicy(readPolicy),keys,Constants.TIME_SERIES_BIN_NAME);
        // uniqueTimestampMap is used to count the unique timestamps
        Map<Long,Integer> uniqueTimestampMap = new HashMap<>();
        // First we need to count timestamps
        for (Record currentRecord : timeSeriesBlocks) {
            // Null record is a possibility if we have just made the current block a historic block
            if (currentRecord != null) {
                @SuppressWarnings("unchecked") // Can assume below casting
                        Map<Long, Double> timeSeries = (Map<Long, Double>) currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME);
                for (Long timestamp : timeSeries.keySet()) {
                    if (timestamp >= startTime && timestamp <= endTime)
                        uniqueTimestampMap.putIfAbsent(timestamp, 1);
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

        for (Record currentRecord : timeSeriesBlocks) {
            // Null record is a possibility if we have just made the current block a historic block
            if (currentRecord != null) {
                @SuppressWarnings("unchecked") // Can assume below casting
                        Map<Long, Double> timeSeries = (Map<Long, Double>) currentRecord.getMap(Constants.TIME_SERIES_BIN_NAME);
                List<Long> sortedList = new ArrayList<>(timeSeries.keySet());
                Collections.sort(sortedList);
                for (Long aSortedList : sortedList) {
                    long timestamp = aSortedList;
                    if ((startTime <= timestamp) && (timestamp <= endTime)) {
                        // Only add one point per timestamp to the array returned
                        // Check below makes sure this happens
                        if (uniqueTimestampMap.get(timestamp) != null) {
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
     * Run a query vs a particular time series range. Query types are as per the enum QueryOperation
     * @param timeSeriesName - time series to run query against
     * @param operation - operation to apply to query e.g. avg, vol, max, min
     * @param fromDateTime - start time for required time series range
     * @param toDateTime - end time for required time series range
     * @return result of the query as a double
     */
    public double runQuery(String timeSeriesName, QueryOperation operation, Date fromDateTime, Date toDateTime) {
        DataPoint[] dataPoints = getPoints(timeSeriesName,fromDateTime, toDateTime);
        switch(operation){
            case MAX:
                double maxValue = Double.MIN_VALUE;
                for(DataPoint d: dataPoints) maxValue = Math.max(maxValue,d.getValue());
                return maxValue == Double.MIN_VALUE ? Double.NaN : maxValue;
            case MIN:
                double minValue = Double.MAX_VALUE;
                for(DataPoint d: dataPoints) minValue = Math.min(minValue,d.getValue());
                return minValue == Double.MAX_VALUE ? Double.NaN : minValue;
            case COUNT:
                return dataPoints.length;
            case AVG:
                double sum = 0;
                double count = dataPoints.length;
                for(DataPoint d: dataPoints) sum+= d.getValue();
                return count > 0 ? sum / count : Double.NaN;
            case VOL:
                sum = 0;
                count = dataPoints.length;
                if(count == 0) return Double.NaN;
                for(DataPoint d: dataPoints) sum+= d.getValue();
                double avg = sum / count;
                double sumsq = 0;
                for(DataPoint d: dataPoints) sumsq+=Math.pow(d.getValue() - avg,2);
                return Math.sqrt(sumsq / count);
            default:
                return Double.NaN;
        }

    }

    /**
     * We store some 'indexes' to make the time series db work.
     * This gives the name of the set they're stored in
     * It simply suffixes the time series set name with TIME_SERIES_INDEX_SET_SUFFIX
     * @return the set name for the time series indexes
     */
    public String timeSeriesIndexSetName(){
        return timeSeriesIndexSetName(timeSeriesSet);
    }

    /**
     * Static method allowing inference of the time index set name - useful in a couple of places elsewhere
     * @param setName - the name of the set that stores the time series data
     * @return - the name of the set to store the indexes in
     */
    public static String timeSeriesIndexSetName(String setName){
        return String.format("%s%s",setName,TIME_SERIES_INDEX_SET_SUFFIX);
    }

    /**
     * Get the earliest timestamp for the given series
     * @param timeSeriesName time series name
     * @return earliest timestamp Long.MAX if series does not exist
     */
    long startTimeForSeries(String timeSeriesName) {
        // First we get the earliest start time from the index block, if it exists
        // Then earliest start time from the current block
        // else Long.MAX - there is no data for this series
        long startTime = Long.MAX_VALUE;

        // Create a policy to make sure, when we look up the first start time from the index that we don't get an error if it doesn't exist
        WritePolicy blockRecordExistsPolicy = new WritePolicy(getWritePolicy());
        // Now try and get the earliest start time from the index
        blockRecordExistsPolicy.filterExp = Exp.build(Exp.binExists(Constants.TIME_SERIES_INDEX_BIN_NAME));

        Record startTimeFromFirstHistoricBlockRecord = asClient.operate(blockRecordExistsPolicy, asKeyForTimeSeriesIndexes(timeSeriesName),
                MapOperation.getByIndex(Constants.TIME_SERIES_INDEX_BIN_NAME, 0, MapReturnType.KEY));

        if (startTimeFromFirstHistoricBlockRecord != null) {
            startTime = startTimeFromFirstHistoricBlockRecord.getLong(Constants.TIME_SERIES_INDEX_BIN_NAME);
        } else {
            WritePolicy currentRecordExistsPolicy = new WritePolicy(getWritePolicy());
            Exp.build(Exp.binExists(Constants.TIME_SERIES_BIN_NAME));

            Record startTimeFromCurrentBlockRecord = asClient.operate(currentRecordExistsPolicy, asCurrentKeyForTimeSeries(timeSeriesName),
                    MapOperation.getByIndex(Constants.TIME_SERIES_BIN_NAME, 0, MapReturnType.KEY));
            if (startTimeFromCurrentBlockRecord != null) {
                startTime = startTimeFromCurrentBlockRecord.getLong(Constants.TIME_SERIES_BIN_NAME);
            }
        }
        return startTime;
    }
    /**
     * Get the latest timestamp for the given series
     * @param timeSeriesName time series name
     * @return latest timestamp Long.MAX if series does not exist
     */
    long endTimeForSeries(String timeSeriesName) {
        // First we get the latest time from the current block.
        // If this does not exist then we get the  end time from the last record in the historic blocks, if it exists
        // else Long.MAX - there is no data for this series
        long endTime = Long.MAX_VALUE;
        // Create a policy to make sure, when we look up the last end time from the current block, we don't get an error if it doesn't exist
        WritePolicy currentRecordExistsPolicy = new WritePolicy(getWritePolicy());
        Exp.build(Exp.binExists(Constants.TIME_SERIES_BIN_NAME));

        Record endTimeFromCurrentBlockRecord = asClient.operate(currentRecordExistsPolicy, asCurrentKeyForTimeSeries(timeSeriesName),
                MapOperation.getByIndex(Constants.TIME_SERIES_BIN_NAME, -1, MapReturnType.KEY));
        // If something is returned, it is the most recent timestamp
        if (endTimeFromCurrentBlockRecord != null) {
            endTime = endTimeFromCurrentBlockRecord.getLong(Constants.TIME_SERIES_BIN_NAME);
        }
        else {
            // Create a policy to make sure, when we look up the last end time from the index that we don't get an error if it doesn't exist
            WritePolicy blockRecordExistsPolicy = new WritePolicy(getWritePolicy());
            blockRecordExistsPolicy.filterExp = Exp.build(Exp.binExists(Constants.TIME_SERIES_INDEX_BIN_NAME));

            // Now try and get the latest start time from the index
            // Get the start time for the most recent historic block
            Record startTimeForLastHistoricBlockRecord = asClient.operate(blockRecordExistsPolicy, asKeyForTimeSeriesIndexes(timeSeriesName),
                    MapOperation.getByIndex(Constants.TIME_SERIES_INDEX_BIN_NAME, -1, MapReturnType.KEY));
            // If it exists
            if (startTimeForLastHistoricBlockRecord != null) {
                long startTimeForLastBlock = startTimeForLastHistoricBlockRecord.getLong(Constants.TIME_SERIES_INDEX_BIN_NAME);
                // Get the last timestamp from that record
                Record endTimeFromLastBlockRecord = asClient.operate(currentRecordExistsPolicy, asKeyForHistoricTimeSeriesBlock(timeSeriesName,startTimeForLastBlock),
                        MapOperation.getByIndex(Constants.TIME_SERIES_BIN_NAME, -1, MapReturnType.KEY));
                endTime = endTimeFromLastBlockRecord.getLong(Constants.TIME_SERIES_BIN_NAME);
            }
        }
        return endTime;
    }

    /**
     * Get the data point count for the series
     * @param timeSeriesName time series name
     * @return data point count for series
     */

    long dataPointCount(String timeSeriesName){
        long dataPointCount = 0;
        // Get the start times from the index block
        Record metadataRecord = asClient.get(getWritePolicy(), asKeyForTimeSeriesIndexes(timeSeriesName),
                Constants.TIME_SERIES_INDEX_BIN_NAME);
        if(metadataRecord != null){
            // If there are any, get the size of each block
            @SuppressWarnings("unchecked") // Type is expected
            Map<Long,Map<String,Long>> metadataMap = (Map<Long,Map<String,Long>>)metadataRecord.getMap(Constants.TIME_SERIES_INDEX_BIN_NAME);
            for(long startTime : metadataMap.keySet()){
                dataPointCount+= metadataMap.get(startTime).get(Constants.ENTRY_COUNT_FIELD_NAME);
            }
        }

        // Create a policy to make sure, when we look up the size from the current block, there are no errors
        WritePolicy currentRecordExistsPolicy = new WritePolicy(getWritePolicy());
        Exp.build(Exp.binExists(Constants.TIME_SERIES_BIN_NAME));

        Record sizeOfCurrentRecord = asClient.operate(currentRecordExistsPolicy, asCurrentKeyForTimeSeries(timeSeriesName),
                MapOperation.size(Constants.TIME_SERIES_BIN_NAME));
        if(sizeOfCurrentRecord != null) dataPointCount+= sizeOfCurrentRecord.getLong(Constants.TIME_SERIES_BIN_NAME);

        return dataPointCount;
    }
}
