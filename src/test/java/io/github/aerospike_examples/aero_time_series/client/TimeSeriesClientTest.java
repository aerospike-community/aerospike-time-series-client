package io.github.aerospike_examples.aero_time_series.client;

import com.aerospike.client.*;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.task.IndexTask;
import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.TestConstants;
import io.github.aerospike_examples.aero_time_series.TestUtilities;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.client.DataPoint;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;
import org.junit.*;

import java.text.SimpleDateFormat;
import java.util.*;

public class TimeSeriesClientTest {
    // Reference base data for creating test time series
    private static final String BASE_DATE = "2022-01-02";
    // Used to parse BASE_DATE
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(DATE_FORMAT);
    // Random data generation object
    private static final Random RANDOM = new Random();

    // Name of test time series
    private static final String TEST_TIME_SERIES_NAME = "TimeSeriesExample";

    // Utility variable to control whether teardown occurs. Use when developing tests
    boolean doTeardown = true;

    // Default TimeSeriesClient for tests
    private static TimeSeriesClient defaultTimeSeriesClient(){
        return new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
    }

    @Before
    // An index on the Metadata map is required
    public void setup(){
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();
        try {
            String indexName =String.format("%s-%s",timeSeriesClient.getTimeSeriesSet(),Constants.METADATA_BIN_NAME);
            IndexTask task = timeSeriesClient.asClient.createIndex(timeSeriesClient.getReadPolicy(),
                    TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, indexName, Constants.METADATA_BIN_NAME,
                    IndexType.STRING, IndexCollectionType.MAPVALUES);
            task.waitTillComplete();
        }
        catch (AerospikeException e){
            //noinspection StatementWithEmptyBody
            if(e.getResultCode() == ResultCode.INDEX_ALREADY_EXISTS){
                // Do nothing
            }
            else
                throw e;
        }

    }

    @Test
    // Can we insert a time series data point and retrieve it
    public void singlePointInsert() throws Exception {
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();
        double tsValue = RANDOM.nextDouble();
        timeSeriesClient.put(TEST_TIME_SERIES_NAME,new DataPoint(getTestBaseDate(),tsValue));
        // Test with the getPoints call
        DataPoint[] dataPointArray = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),getTestBaseDate());
        Assert.assertEquals(1, dataPointArray.length);
        Assert.assertTrue(dataPointArray[0].equals(new DataPoint(getTestBaseDate(),tsValue)));

        // Test with getPoint
        DataPoint dataPoint = timeSeriesClient.getPoint(TEST_TIME_SERIES_NAME,getTestBaseDate());
        Assert.assertTrue(dataPoint.equals(new DataPoint(getTestBaseDate(),tsValue)));
    }

    @Test
    // Check we get an empty array / null point when no points found
    public void nullBehaviourWhenPointsNotAvailable() throws Exception{
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        DataPoint[] dataPointArray = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),getTestBaseDate());
        Assert.assertEquals(dataPointArray.length,0);

        DataPoint dataPoint = timeSeriesClient.getPoint(TEST_TIME_SERIES_NAME,getTestBaseDate());
        Assert.assertNull(dataPoint);
    }

    @Test
    // When we insert multiple points
    // Do we get them back in the correct order
    public void multiplePointInsert() throws Exception{
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = createTimeSeries(TEST_TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),
                new Date(getTestBaseDate().getTime() + (dataPointCount - 1) * timeIncrementInSeconds * Constants.MILLISECONDS_IN_SECOND));
        for(int i=0;i<dataPointCount;i++){
            DataPoint shouldBeDataPoint = new DataPoint(
                    new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds * Constants.MILLISECONDS_IN_SECOND),values[i]);
            Assert.assertTrue(dataPoints[i].equals(shouldBeDataPoint));
        }
    }

    @Test
    // When we insert multiple points out of sequence
    // Do we get them back in the correct order
    public void multiplePointInsert2() throws Exception{
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = new double[dataPointCount];
        for(int i=dataPointCount - 1;i>=0;i--){
            double value = RANDOM.nextDouble();
            values[i] = value;
            timeSeriesClient.put(TEST_TIME_SERIES_NAME,new DataPoint(new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds ),value));
        }
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),new Date(getTestBaseDate().getTime() + (dataPointCount - 1) * timeIncrementInSeconds));
        for(int i=0;i<dataPointCount;i++){
            DataPoint shouldBeDataPoint = new DataPoint(new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds),values[i]);
            Assert.assertTrue(dataPoints[i].equals(shouldBeDataPoint));
        }
    }

    @Test
    // Time bounds respected for getPoints
    // If only first n points requested, only return first n
    public void timeBoundsRespected1() throws Exception{
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = createTimeSeries(TEST_TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        // Should only get first five data points
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),
                new Date(getTestBaseDate().getTime() + (dataPointCount/2 - 1) * timeIncrementInSeconds * Constants.MILLISECONDS_IN_SECOND));
        // Check the count is correct
        Assert.assertEquals(dataPoints.length, dataPointCount / 2);
        // and the points are the ones we expect
        for(int i=0;i<dataPointCount/2 ;i++){
            DataPoint shouldBeDataPoint = new DataPoint(
                    new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds * Constants.MILLISECONDS_IN_SECOND),values[i]);
            Assert.assertTrue(dataPoints[i].equals(shouldBeDataPoint));
        }
    }

    @Test
    // Time bounds respected for getPoints
    // If only last n points requested, only return last n
    public void timeBoundsRespected2() throws Exception{
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = createTimeSeries(TEST_TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        // Should only get second five data points
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,
                new Date(getTestBaseDate().getTime() + (dataPointCount / 2) * timeIncrementInSeconds * Constants.MILLISECONDS_IN_SECOND),
                new Date(getTestBaseDate().getTime() + dataPointCount * timeIncrementInSeconds * Constants.MILLISECONDS_IN_SECOND));
        // Check the count is correct
        Assert.assertEquals(dataPoints.length, dataPointCount / 2);
        // and the points are the ones we expect
        for(int i=dataPointCount / 2;i<dataPointCount ;i++){
            DataPoint shouldBeDataPoint = new DataPoint(
                    new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds * Constants.MILLISECONDS_IN_SECOND),values[i]);
            Assert.assertTrue(dataPoints[i - dataPointCount/2].equals(shouldBeDataPoint));
        }
    }

    @Test
    // Check that block creation happens correctly
    // Do we get n blocks when no of data points is n * m, where m is allowed entries per block
    public void blockTest() throws Exception{
        int entriesPerBlock = 60;
        int requiredBlocks = 10;
        doTeardown = false;
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,
                Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);

        createTimeSeries(TEST_TIME_SERIES_NAME,1,requiredBlocks * entriesPerBlock,entriesPerBlock);

        Assert.assertEquals(TestUtilities.blockCountForTimeseries(timeSeriesClient,TestConstants.AEROSPIKE_NAMESPACE,TEST_TIME_SERIES_NAME),requiredBlocks);
    }

    @Test
    /**
     * Check correct time series points are retrieved
     * There are number of cases to consider
     * i) start time / end time does not coincide with a block start time
     * ii) start time coincides with a block start time, end time does not
     * iii) start time does not coincide with a block start time but end time does not
     * iv) start time coincides with start time for time series, end time does not coincide with a block start time
     * v) start time precedes start time for time series, end time does not coincide with a block start time
     * vi) start time is after start time for series, but does not coincide with a block boundary. End time is beyond last recorded time for series
     * vii end time is coincides with a block start time - get an extra block
     * viii start time and end time are the same, do not coincide with start of a block
     * ix start time and end time are the same, coincide with start of a block
     * x start time and end time are beyond the last historic block
     *
     * These are in a separate function (see doCorrectSeriesForTimeRangeCheck) as the same checks are used twice
     */
    public void correctSeriesForTimeRange() throws Exception{
        int entriesPerBlock = 60;
        int requiredBlocks = 10;
        createTimeSeries(TEST_TIME_SERIES_NAME,1,requiredBlocks * entriesPerBlock,entriesPerBlock);
        doCorrectSeriesForTimeRangeChecks();
    }

    @Test
    /**
     * Check correct time series points are retrieved when race conditions are introduced into the copyBlock process
     *
     * The same checks are used as for correctSeriesForTimeRange
     *
     * The race condition's introduction can be seen in TimeSeriesClient.copyCurrentDataToHistoricBlock
     *
     * It is enabled by
     */
    public void correctSeriesForTimeRangeWithRaceConditions() throws Exception{
        int entriesPerBlock = 60;
        int requiredBlocks = 10;
        createTimeSeries(TEST_TIME_SERIES_NAME,1,requiredBlocks * entriesPerBlock,entriesPerBlock,true,20);
        doCorrectSeriesForTimeRangeChecks();
    }

    private void doCorrectSeriesForTimeRangeChecks() throws Exception{
        checkCorrectSeriesForTimeRange(30,90,90-30+1);
        checkCorrectSeriesForTimeRange(60,150,150-60+1);
        checkCorrectSeriesForTimeRange(90,179,179-90+1);
        //noinspection PointlessArithmeticExpression = deliberate to make it clear where expected count is coming from
        checkCorrectSeriesForTimeRange(0,150,150-0+1);
        checkCorrectSeriesForTimeRange(-100,150,150+1);
        checkCorrectSeriesForTimeRange(90,700,600-90);
        checkCorrectSeriesForTimeRange(30,60,60-30+1);
        checkCorrectSeriesForTimeRange(30,20,0);
        checkCorrectSeriesForTimeRange(30,30,1);
        checkCorrectSeriesForTimeRange(60,60,1);
    }

    /*
        This function to be used in conjunction with correctSeriesForTimeRange only
        Checks that the series retrieved obeys the following rules
        1)  No of points is expected
        2)  For each point
                The timestamp is within the required time range
                Each point's timestamp is greater than the one preceding it
     */
    private void checkCorrectSeriesForTimeRange(int startTimeOffsetInSeconds,int endTimeOffsetInSeconds,int expectedCount) throws Exception {
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        Date startTime = new Date(getTestBaseDate().getTime() + startTimeOffsetInSeconds * Constants.MILLISECONDS_IN_SECOND);
        Date endTime = new Date(getTestBaseDate().getTime() + endTimeOffsetInSeconds * Constants.MILLISECONDS_IN_SECOND);
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,startTime,endTime);
        Assert.assertEquals(dataPoints.length,expectedCount);
        for(int i=0;i<dataPoints.length;i++){
            Assert.assertTrue(dataPoints[i].getTimestamp() >= startTime.getTime());
            Assert.assertTrue(dataPoints[i].getTimestamp() <= endTime.getTime());
            if(i>0) Assert.assertTrue(dataPoints[i].getTimestamp() > dataPoints[i-1].getTimestamp());
        }
    }
    @Test
    /**
     * Check correct blocks are retrieved for query intervals
     * There are number of cases to consider
     * i) start time / end time does not coincide with a block start time
     * ii) start time coincides with a block start time, end time does not
     * iii) start time does not coincide with a block start time but end time does not
     * iv) start time coincides with start time for time series, end time does not coincide with a block start time
     * v) start time precedes start time for time series, end time does not coincide with a block start time
     * vi) start time is after start time for series, but does not coincide with a block boundary. End time is beyond last recorded time for series
     * vii end time is coincides with a block start time - get an extra block
     * viii start time and end time are the same, do not coincide with start of a block
     * ix start time and end time are the same, coincide with start of a block
     * x start time and end time are beyond the last historic block
     */
    public void correctBlocksForTimeRange() throws Exception{
        int entriesPerBlock = 60;
        int requiredBlocks = 10;
        createTimeSeries(TEST_TIME_SERIES_NAME,1,requiredBlocks * entriesPerBlock,entriesPerBlock);

        checkCorrectBlocksForTimeRange(30,90,2,false);
        checkCorrectBlocksForTimeRange(60,150,2,false);
        checkCorrectBlocksForTimeRange(90,179 ,2,false);
        checkCorrectBlocksForTimeRange(0,150,3,false);
        checkCorrectBlocksForTimeRange(-100,150,3,false);
        checkCorrectBlocksForTimeRange(90,700,10,true);
        checkCorrectBlocksForTimeRange(30,60,2,false);
        checkCorrectBlocksForTimeRange(30,20,0,false);
        checkCorrectBlocksForTimeRange(30,30,1,false);
        checkCorrectBlocksForTimeRange(60,60,1,false);
        checkCorrectBlocksForTimeRange(600,650,2,true);

    }

    @Test
    /**
     * Check that our query functions do what they are supposed to do
     * i.e. Correct average, count, max, min,vol
     * We do this by doing the calculations directly, and comparing them with the api results
     */
    public void checkQueryFunctions() throws Exception{
        int entriesPerBlock = 60;
        int requiredBlocks = 10;
        int intervalInSeconds = 15;

        long startTime = getTestBaseDate().getTime();
        long endTime = startTime + entriesPerBlock * requiredBlocks * intervalInSeconds * Constants.MILLISECONDS_IN_SECOND;
        // Raw values - before they're added to the database
        double[] tsValues = createTimeSeries(TEST_TIME_SERIES_NAME,intervalInSeconds,requiredBlocks * entriesPerBlock,entriesPerBlock);
        // Aggregates, as computed by the API
        double queryAvgValue = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME,TimeSeriesClient.QueryOperation.AVG,new Date(startTime), new Date(endTime));
        double queryCount = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME,TimeSeriesClient.QueryOperation.COUNT,new Date(startTime), new Date(endTime));
        double queryMax = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME,TimeSeriesClient.QueryOperation.MAX,new Date(startTime), new Date(endTime));
        double queryMin = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME,TimeSeriesClient.QueryOperation.MIN,new Date(startTime), new Date(endTime));
        double queryVol = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME,TimeSeriesClient.QueryOperation.VOL,new Date(startTime), new Date(endTime));

        // Aggregates - as computed 'by hand'
        double foundSum = 0;
        double foundSumSq = 0;
        double foundMax = Double.MIN_VALUE;
        double foundMin = Double.MAX_VALUE;

        for(double tsValue : tsValues){
            foundSum += tsValue;
            foundMax = Math.max(foundMax,tsValue);
            foundMin = Math.min(foundMin,tsValue);
            foundSumSq += Math.pow(tsValue,2);
        }

        double foundAvg = foundSum / tsValues.length;
        double foundVol = Math.sqrt((foundSumSq - tsValues.length * Math.pow(foundAvg,2)) / tsValues.length);

        // Check that the two sets of aggregates agree
        Assert.assertEquals(tsValues.length, queryCount, 0.0);
        Assert.assertEquals(foundSum / tsValues.length, queryAvgValue, 0.0);
        Assert.assertEquals(foundMax, queryMax, 0.0);
        Assert.assertEquals(foundMin, queryMin, 0.0);
        // Need to compare volatility using a tolerance - rounding makes the values different
        Assert.assertTrue(Utilities.valueInTolerance(queryVol,foundVol,0.001));
    }

    @Test
    /**
     * Check that our query functions are well behaved when there's zero data
     * i.e. Correct average, count, max, min,vol
     */
    public void checkQueryFunctionsBehaveForZeroData() throws Exception {
        int entriesPerBlock = 60;
        int requiredBlocks = 10;
        int intervalInSeconds = 15;

        long startTime = getTestBaseDate().getTime();
        long endTime = startTime -1;
        // Raw values - before they're added to the database
        double[] tsValues = createTimeSeries(TEST_TIME_SERIES_NAME, intervalInSeconds, requiredBlocks * entriesPerBlock, entriesPerBlock);
        // Aggregates, as computed by the API
        double queryAvgValue = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME, TimeSeriesClient.QueryOperation.AVG, new Date(startTime), new Date(endTime));
        double queryCount = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME, TimeSeriesClient.QueryOperation.COUNT, new Date(startTime), new Date(endTime));
        double queryMax = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME, TimeSeriesClient.QueryOperation.MAX, new Date(startTime), new Date(endTime));
        double queryMin = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME, TimeSeriesClient.QueryOperation.MIN, new Date(startTime), new Date(endTime));
        double queryVol = defaultTimeSeriesClient().runQuery(TEST_TIME_SERIES_NAME, TimeSeriesClient.QueryOperation.VOL, new Date(startTime), new Date(endTime));

        Assert.assertTrue(Double.isNaN(queryAvgValue));
        Assert.assertTrue(Double.isNaN(queryVol));
        Assert.assertTrue(Double.isNaN(queryMax));
        Assert.assertTrue(Double.isNaN(queryMin));
        Assert.assertEquals(0, queryCount, 0.0);
    }
    /*
        This function to be used in conjunction with correctBlocksForTimeRange only
        Checks that the start times for the time series blocks we retrieve obey the following rules
        1)  First timestamp is
            i)  equal to expected timestamp or
            ii) the earliest timestamp available for the series or
            iii) first timestamp is less than the required timestamp and second one is greater
            iv) there are only two timestamps and second one has CURRENT_RECORD_TIMESTAMP marker - means start timestamp is beyond last timestamp
        2)  End timestamp is greater than the last timestamp
        3)  The number of blocks retrieved is the expected number
        4)  Whether the last timestamp has the special CURRENT_RECORD_TIMESTAMP marker
     */
    private void checkCorrectBlocksForTimeRange(int startTimeOffsetInSeconds,int endTimeOffsetInSeconds,int expectedBlocks, boolean lastBlockZero) throws Exception{
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        long startTimeAsTimestamp = getTestBaseDate().getTime() +startTimeOffsetInSeconds * Constants.MILLISECONDS_IN_SECOND;
        long endTimeAsTimestamp = getTestBaseDate().getTime()+endTimeOffsetInSeconds * Constants.MILLISECONDS_IN_SECOND;
        long[] timestamps = timeSeriesClient.getTimestampsForTimeSeries(TEST_TIME_SERIES_NAME,startTimeAsTimestamp,endTimeAsTimestamp);
        int indexOfLastRecord = timestamps.length - 1;
        if(startTimeOffsetInSeconds <= endTimeOffsetInSeconds) {
            Assert.assertTrue(timestamps.length == expectedBlocks);
            Assert.assertTrue(timestamps[0] == startTimeAsTimestamp ||
                    timestamps[0] == getTestBaseDate().getTime() ||
                    (timestamps[1] > startTimeAsTimestamp && timestamps[0] < startTimeAsTimestamp) ||
                    ((timestamps.length ==2 ) && (timestamps[1] == TimeSeriesClient.CURRENT_RECORD_TIMESTAMP)));
            Assert.assertTrue(timestamps[indexOfLastRecord] <= endTimeAsTimestamp);
            Assert.assertTrue((timestamps[indexOfLastRecord] == TimeSeriesClient.CURRENT_RECORD_TIMESTAMP) == lastBlockZero);
        }
        else
            Assert.assertTrue(timestamps.length ==0);

    }

    /**
     * Utility method to create time series
     * Returns an array of the random values generated
     *
     * @param timeSeriesName - name of the time series
     * @param intervalInSeconds - time interval between data points
     * @param iterations - no of data points
     * @param recordsPerBlock - records per block
     * @param useTestMode - enable code that simulates race conditions when testing
     * @param failurePctForCopyBlock - percentage of 'copy block' calls that fail due to simulated race conditions
     * @return
     * @throws Exception
     */
    private double[] createTimeSeries(String timeSeriesName,int intervalInSeconds,int iterations, int recordsPerBlock,
            boolean useTestMode, double failurePctForCopyBlock) throws Exception{

        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,recordsPerBlock);
        timeSeriesClient.testMode = useTestMode;
        timeSeriesClient.failurePctRateForCopyBlock = failurePctForCopyBlock;

        double[] values = new double[iterations];
        for(int i=0;i<iterations;i++){
            double value = RANDOM.nextDouble();
            values[i] = value;
            timeSeriesClient.put(timeSeriesName,new DataPoint(new Date(getTestBaseDate().getTime() + i * intervalInSeconds * Constants.MILLISECONDS_IN_SECOND),value));
        }
        return values;
    }

    /**
     * Utility method to create time series
     * Returns an array of the random values generated
     * This is the 'normal' version of createTimeSeries - it does not introduce any race conditions
     *
     * @param timeSeriesName - name of the time series
     * @param intervalInSeconds - time interval between data points
     * @param iterations - no of data points
     * @param recordsPerBlock - records per block
     * @return
     * @throws Exception
     */

    private double[] createTimeSeries(String timeSeriesName,int intervalInSeconds,int iterations, int recordsPerBlock) throws Exception{
        return createTimeSeries(timeSeriesName, intervalInSeconds, iterations,recordsPerBlock,false,0);
    }

    /**
     * Utility method to create time series
     * Returns an array of the random values generated
     * Time series data points per block will be the default value - Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK
     *
     * @param timeSeriesName - name of the time series
     * @param intervalInSeconds - time interval between data points
     * @param iterations - no of data points
     * @return
     * @throws Exception
     */
    private double[] createTimeSeries(String timeSeriesName,int intervalInSeconds,int iterations) throws Exception{
        return createTimeSeries(timeSeriesName, intervalInSeconds, iterations, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
    }

    // Utility method - we need a base date for our time series generation
    private Date getTestBaseDate() throws Exception{
        return DATE_FORMATTER.parse(BASE_DATE);
    }

    @After
    // Truncate the time series set
    // Use of null means truncate whole set
    public void teardown(){
        TimeSeriesClient timeSeriesClient = defaultTimeSeriesClient();

        if(doTeardown) {
            timeSeriesClient.asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, timeSeriesClient.getTimeSeriesSet(), null);
            timeSeriesClient.asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, timeSeriesClient.timeSeriesIndexSetName(), null);
        }
    }

    @Test
    /*
        Insert one record, so we have some pre-existing records for the series
        Then insert 10 records with maxEntryCount = 3
        In aggregate we should have 11 records, spread across four blocks

        1) Check that we have four blocks and the last block is a current block. Should have two records in the current block.
            Full time range query should retrieve 11 points
        2) Add one more data point - should have at least four blocks (note checkCurrentBlocks will return 5 here). Should have zero records in current block
            Full time range query should retrieve 12 points
        3) Should be able to handle a zero length data point array without exception
     */
    public void bulkLoadTest() throws Exception{
        long startTime = getTestBaseDate().getTime();
        int recordsPerBlock = 3;
        TimeSeriesClient timeSeriesClient;

        timeSeriesClient = new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,recordsPerBlock);
        DataPoint[] dataPoints = createDataPoints(startTime,1,1);
        timeSeriesClient.put(TEST_TIME_SERIES_NAME,dataPoints);

        // Test 1
        //noinspection PointlessArithmeticExpression - making at clear start time addition is 1 second by design
        startTime += 1 * Constants.MILLISECONDS_IN_SECOND;
        dataPoints = createDataPoints(startTime,1,10);
        timeSeriesClient.put(TEST_TIME_SERIES_NAME,dataPoints);

        checkCorrectBlocksForTimeRange(0, 11,4,true);
        checkCorrectSeriesForTimeRange(0,11,11);
        Assert.assertTrue(checkCurrentRecordCount(timeSeriesClient,TEST_TIME_SERIES_NAME) == 2);

        // Test 2
        startTime += 10 * Constants.MILLISECONDS_IN_SECOND;
        dataPoints = createDataPoints(startTime,1,1);
        timeSeriesClient.put(TEST_TIME_SERIES_NAME,dataPoints);

        checkCorrectBlocksForTimeRange(0, 12,5,true);
        checkCorrectSeriesForTimeRange(0,12,12);
        Assert.assertTrue(checkCurrentRecordCount(timeSeriesClient,TEST_TIME_SERIES_NAME) == 0);

        // Test 3
        timeSeriesClient.put(TEST_TIME_SERIES_NAME,new DataPoint[0]);
    }

    private static int checkCurrentRecordCount(TimeSeriesClient timeSeriesClient, String timeSeriesName){
        int currentRecordCount = 0;
        Record r = timeSeriesClient.asClient.operate(new WritePolicy(),timeSeriesClient.asCurrentKeyForTimeSeries(timeSeriesName),
                MapOperation.size(Constants.TIME_SERIES_BIN_NAME));
        if(r != null) currentRecordCount = r.getInt(Constants.TIME_SERIES_BIN_NAME);
        return currentRecordCount;
    }
    private DataPoint[] createDataPoints(long startTime,int intervalInSeconds,int iterations) {
        DataPoint[] dataPoints = new DataPoint[iterations];
        for(int i=0;i<iterations;i++){
            long timestamp  = startTime + i * intervalInSeconds * Constants.MILLISECONDS_IN_SECOND;
            double value = RANDOM.nextDouble();
            dataPoints[i] = new DataPoint(new Date(timestamp),value);
        }
        return dataPoints;
    }
}
