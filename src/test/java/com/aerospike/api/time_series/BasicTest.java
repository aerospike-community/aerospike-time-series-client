package com.aerospike.api.time_series;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.task.IndexTask;
import org.junit.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class BasicTest {
    // TimeSeriesClient object used for testing
    private static TimeSeriesClient timeSeriesClient;
    // Reference base data for creating test time series
    private static final String BASE_DATE = "2022-01-02";
    // Used to parse BASE_DATE
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(DATE_FORMAT);
    // Random data generation object
    private static final Random RANDOM = new Random();

    // Name of test time series
    private static final String TEST_TIME_SERIES_NAME = "TimeSeriesExample";


    @Before
    // An index on the Metadata map is required
    public void setup(){
        try {
            IndexTask task = timeSeriesClient.asClient.createIndex(null,
                    TestConstants.AEROSPIKE_NAMESPACE, Constants.AS_TIME_SERIES_SET, Constants.METADATA_BIN_NAME, Constants.METADATA_BIN_NAME,
                    IndexType.STRING, IndexCollectionType.MAPVALUES);
            task.waitTillComplete();
        }
        catch (AerospikeException e){
            if(e.getResultCode() == ResultCode.INDEX_ALREADY_EXISTS){
                // Do nothing
            }
        }

    }

    @BeforeClass
    // Get a time series client
    public static void init(){
        timeSeriesClient = new TimeSeriesClient(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE);
    }

    @Test
    // Can we insert a time series data point and retrieve it
    public void singlePointInsert() throws Exception {
        double tsValue = RANDOM.nextDouble();
        timeSeriesClient.put(TEST_TIME_SERIES_NAME,new DataPoint(getTestBaseDate(),tsValue));
        // Test with the getPoints call
        DataPoint[] dataPointArray = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),getTestBaseDate());
        Assert.assertTrue(dataPointArray.length == 1);
        Assert.assertTrue(dataPointArray[0].equals(new DataPoint(getTestBaseDate(),tsValue)));

        // Test with getPoint
        DataPoint dataPoint = timeSeriesClient.getPoint(TEST_TIME_SERIES_NAME,getTestBaseDate());
        Assert.assertTrue(dataPoint.equals(new DataPoint(getTestBaseDate(),tsValue)));
    }

    @Test
    // Check we get an empty array / null point when no points found
    public void nullBehaviourWhenPointsNotAvailable() throws Exception{
        DataPoint[] dataPointArray = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),getTestBaseDate());
        Assert.assertEquals(dataPointArray.length,0);

        DataPoint dataPoint = timeSeriesClient.getPoint(TEST_TIME_SERIES_NAME,getTestBaseDate());
        Assert.assertNull(dataPoint);
    }

    @Test
    // When we insert multiple points
    // Do we get them back in the correct order
    public void multiplePointInsert() throws Exception{
        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = createTimeSeries(TEST_TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),new Date(getTestBaseDate().getTime() + (dataPointCount - 1) * timeIncrementInSeconds));
        for(int i=0;i<dataPointCount;i++){
            DataPoint shouldBeDataPoint = new DataPoint(new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds),values[i]);
            Assert.assertTrue(dataPoints[i].equals(shouldBeDataPoint));
        }
    }

    @Test
    // When we insert multiple points out of sequence
    // Do we get them back in the correct order
    public void multiplePointInsert2() throws Exception{
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
        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = createTimeSeries(TEST_TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        // Should only get first five data points
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,getTestBaseDate(),new Date(getTestBaseDate().getTime() + (dataPointCount/2 - 1) * timeIncrementInSeconds));
        // Check the count is correct
        Assert.assertTrue(dataPoints.length == dataPointCount / 2 );
        // and the points are the ones we expect
        for(int i=0;i<dataPointCount/2 ;i++){
            DataPoint shouldBeDataPoint = new DataPoint(new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds),values[i]);
            Assert.assertTrue(dataPoints[i].equals(shouldBeDataPoint));
        }
    }

    @Test
    // Time bounds respected for getPoints
    // If only last n points requested, only return last n
    public void timeBoundsRespected2() throws Exception{
        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = createTimeSeries(TEST_TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        // Should only get second five data points
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TEST_TIME_SERIES_NAME,
                new Date(getTestBaseDate().getTime() + (dataPointCount / 2) * timeIncrementInSeconds),
                new Date(getTestBaseDate().getTime() + dataPointCount * timeIncrementInSeconds));
        // Check the count is correct
        Assert.assertTrue(dataPoints.length == dataPointCount / 2 );
        // and the points are the ones we expect
        for(int i=dataPointCount / 2;i<dataPointCount ;i++){
            DataPoint shouldBeDataPoint = new DataPoint(new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds),values[i]);
            Assert.assertTrue(dataPoints[i - dataPointCount/2].equals(shouldBeDataPoint));
        }
    }

    @Test
    // Check that block creation happens correctly
    // Do we get n blocks when no of data points is n * m, where m is allowed entries per block
    public void blockTest() throws Exception{
        int entriesPerBlock = 10;
        int requiredBlocks = 3;
        createTimeSeries(TEST_TIME_SERIES_NAME,60,requiredBlocks * entriesPerBlock,entriesPerBlock);

        Statement stmt = new Statement();
        stmt.setNamespace(TestConstants.AEROSPIKE_NAMESPACE);
        stmt.setSetName(Constants.AS_TIME_SERIES_SET);
        stmt.setBinNames(Constants.METADATA_BIN_NAME);
        stmt.setFilter(Filter.contains(Constants.METADATA_BIN_NAME, IndexCollectionType.MAPVALUES, TEST_TIME_SERIES_NAME));

        RecordSet rs = timeSeriesClient.asClient.query(null, stmt);
        int count = 0;
        while(rs.next()) count++;
        Assert.assertEquals(count,requiredBlocks);
    }

    /**
     * Utility method to create time series
     * Returns an array of the random values generated
     *
     * @param timeSeriesName - name of the time series
     * @param intervalInSeconds - time interval between data points
     * @param iterations - no of data points
     * @param recordsPerBlock - records per block
     * @return
     * @throws Exception
     */
    private double[] createTimeSeries(String timeSeriesName,int intervalInSeconds,int iterations, int recordsPerBlock) throws Exception{
        double[] values = new double[iterations];
        for(int i=0;i<iterations;i++){
            double value = RANDOM.nextDouble();
            values[i] = value;
            timeSeriesClient.put(timeSeriesName,new DataPoint(new Date(getTestBaseDate().getTime() + i * intervalInSeconds ),value),recordsPerBlock);
        }
        return values;
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

    //@After
    // Truncate the time series set
    public void teardown(){
        timeSeriesClient.asClient.truncate(new InfoPolicy(),TestConstants.AEROSPIKE_NAMESPACE,Constants.AS_TIME_SERIES_SET,null);
    }

}
