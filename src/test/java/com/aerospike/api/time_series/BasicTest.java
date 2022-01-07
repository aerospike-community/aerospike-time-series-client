package com.aerospike.api.time_series;

import org.junit.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import com.aerospike.client.policy.InfoPolicy;

public class BasicTest {
    private static Client timeSeriesClient;
    private static final String BASE_DATE = "2022-01-02";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(DATE_FORMAT);
    private static final Random RANDOM = new Random();
    private static final String TIME_SERIES_NAME = "TimeSeriesExample";


    @BeforeClass
    // Get a time series client
    public static void init(){
        timeSeriesClient = new Client(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE);
    }

    @Test
    // Can we insert a time series data point and retrieve it
    public void singlePointInsert() throws Exception {
        double tsValue = RANDOM.nextDouble();
        timeSeriesClient.put(TIME_SERIES_NAME,new DataPoint(getTestBaseDate(),tsValue));
        // Test with the getPoints call
        DataPoint[] dataPointArray = timeSeriesClient.getPoints(TIME_SERIES_NAME,getTestBaseDate(),getTestBaseDate());
        Assert.assertTrue(dataPointArray.length == 1);
        Assert.assertTrue(dataPointArray[0].equals(new DataPoint(getTestBaseDate(),tsValue)));

        // Test with getPoint
        DataPoint dataPoint = timeSeriesClient.getPoint(TIME_SERIES_NAME,getTestBaseDate());
        Assert.assertTrue(dataPoint.equals(new DataPoint(getTestBaseDate(),tsValue)));
    }

    @Test
    // Check we get an empty array / null point when no points found
    public void nullBehaviourWhenPointsNotAvailable() throws Exception{
        DataPoint[] dataPointArray = timeSeriesClient.getPoints(TIME_SERIES_NAME,getTestBaseDate(),getTestBaseDate());
        Assert.assertEquals(dataPointArray.length,0);

        DataPoint dataPoint = timeSeriesClient.getPoint(TIME_SERIES_NAME,getTestBaseDate());
        Assert.assertNull(dataPoint);
    }

    @Test
    // When we insert multiple points
    // Do we get them back in the correct order
    public void multiplePointInsert() throws Exception{
        int dataPointCount = 10;
        int timeIncrementInSeconds = 15;
        double[] values = createTimeSeries(TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TIME_SERIES_NAME,getTestBaseDate(),new Date(getTestBaseDate().getTime() + (dataPointCount - 1) * timeIncrementInSeconds));
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
            timeSeriesClient.put(TIME_SERIES_NAME,new DataPoint(new Date(getTestBaseDate().getTime() + i * timeIncrementInSeconds ),value));
        }
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TIME_SERIES_NAME,getTestBaseDate(),new Date(getTestBaseDate().getTime() + (dataPointCount - 1) * timeIncrementInSeconds));
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
        double[] values = createTimeSeries(TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        // Should only get first five data points
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TIME_SERIES_NAME,getTestBaseDate(),new Date(getTestBaseDate().getTime() + (dataPointCount/2 - 1) * timeIncrementInSeconds));
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
        double[] values = createTimeSeries(TIME_SERIES_NAME,timeIncrementInSeconds,dataPointCount);
        // Should only get second five data points
        DataPoint[] dataPoints = timeSeriesClient.getPoints(TIME_SERIES_NAME,
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

    @After
    // Truncate the time series set
    public void teardown(){
        timeSeriesClient.asClient.truncate(new InfoPolicy(),TestConstants.AEROSPIKE_NAMESPACE,Constants.AS_TIME_SERIES_SET,null);
    }

    // Utility method to create time series
    // Returns an array of the random values generated
    private double[] createTimeSeries(String timeSeriesName,int intervalInSeconds,int iterations) throws Exception{
        double[] values = new double[iterations];
        for(int i=0;i<iterations;i++){
            double value = RANDOM.nextDouble();
            values[i] = value;
            timeSeriesClient.put(timeSeriesName,new DataPoint(new Date(getTestBaseDate().getTime() + i * intervalInSeconds ),value));
        }
        return values;
    }
    // Utility method - we need a base date for our time series generation
    private Date getTestBaseDate() throws Exception{
        return DATE_FORMATTER.parse(BASE_DATE);
    }

}
