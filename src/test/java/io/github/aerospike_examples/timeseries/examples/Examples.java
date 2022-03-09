package io.github.aerospike_examples.timeseries.examples;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.QueryOperation;
import io.github.aerospike_examples.timeseries.TestConstants;
import io.github.aerospike_examples.timeseries.TestUtilities;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;
import io.github.aerospike_examples.timeseries.TimeSeriesInfo;
import io.github.aerospike_examples.timeseries.benchmarker.TimeSeriesSimulator;
import io.github.aerospike_examples.timeseries.util.Constants;
import io.github.aerospike_examples.timeseries.util.Utilities;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Examples {
    
    /**
     * Simple time series insertion example
     */
    @Test
    public void simpleTimeSeriesExample() throws ParseException {
        TestUtilities.removeTimeSeriesTestDataForSet(TestConstants.TIME_SERIES_TEST_SET);
        AerospikeClient asClient = new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT);
        String asNamespaceName = "test";
        // Let's store some temperature readings taken in Trafalgar Square, London. Readings are Centigrade.
        String timeSeriesName = "TrafalgarSquare-Temperature-Centigrade";
        // The readings were taken on the 14th Feb, 2022
        Date observationDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-02-14");
        // ... and here they are
        double[] hourlyTemperatureObservations =
                new double[]{2.7, 2.3, 1.9, 1.8, 1.8, 1.7, 2.3, 3.2, 4.7, 5.4, 6.3, 7.7, 7.9, 9.9, 9.3,
                        9.6, 9.7, 8.4, 7.4, 6.8, 5.5, 5.4, 4.3, 4.2};

        // To store, create a time series client object. Requires AerospikeClient object and Aerospike namespace name
        // new TimeSeriesClient(AerospikeClient asClient, String asNamespaceName)
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(asClient, asNamespaceName);
        // Insert our hourly temperature readings
        for (int i = 0; i < hourlyTemperatureObservations.length; i++) //noinspection SpellCheckingInspection
        {
            // The dataPoint consists of the base date + the required number of hours
            DataPoint dataPoint = new DataPoint(
                    Utilities.incrementDateUsingSeconds(observationDate, i * 3600),
                    hourlyTemperatureObservations[i]);
            // Which we then 'put'
            timeSeriesClient.put(timeSeriesName, dataPoint);
        }
        TimeSeriesInfo timeSeriesInfo = TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient, timeSeriesName);
        System.out.println(timeSeriesInfo);

        timeSeriesClient.printTimeSeries(timeSeriesName);

        System.out.println(
                String.format("Maximum temperature is %.3f",
                        timeSeriesClient.runQuery(timeSeriesName,
                                QueryOperation.MAX,
                                timeSeriesInfo.getStartDateTime(), timeSeriesInfo.getEndDateTime())));
    }

    /**
     * Batch time series insertion example
     *
     * @throws ParseException - in theory, in practice no
     */
    @Test
    public void batchTimeSeriesExample() throws ParseException {
        TestUtilities.removeTimeSeriesTestDataForSet(TestConstants.TIME_SERIES_TEST_SET);
        AerospikeClient asClient = new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT);
        String asNamespaceName = "test";
        // Let's store some temperature readings taken in Trafalgar Square, London. Readings are Centigrade.
        String timeSeriesName = "TrafalgarSquare-Temperature-Centigrade";
        // The readings were taken on the 14th Feb, 2022
        Date observationDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-02-14");
        // ... and here they are
        double[] hourlyTemperatureObservations =
                new double[]{2.7, 2.3, 1.9, 1.8, 1.8, 1.7, 2.3, 3.2, 4.7, 5.4, 6.3, 7.7, 7.9, 9.9, 9.3,
                        9.6, 9.7, 8.4, 7.4, 6.8, 5.5, 5.4, 4.3, 4.2};
        // To store, create a time series client object. Requires AerospikeClient object and Aerospike namespace name
        // new TimeSeriesClient(AerospikeClient asClient, String asNamespaceName)
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(asClient, asNamespaceName);
        // Insert our hourly temperature readings
        DataPoint[] dataPoints = new DataPoint[hourlyTemperatureObservations.length];
        for (int i = 0; i < hourlyTemperatureObservations.length; i++) //noinspection SpellCheckingInspection
        {
            // The dataPoint consists of the base date + the required number of hours
            dataPoints[i] = new DataPoint(
                    Utilities.incrementDateUsingSeconds(observationDate, i * 3600),
                    hourlyTemperatureObservations[i]);
        }
        timeSeriesClient.put(timeSeriesName, dataPoints);

        TimeSeriesInfo timeSeriesInfo = TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient, timeSeriesName);
        System.out.println(timeSeriesInfo);

        timeSeriesClient.printTimeSeries(timeSeriesName);

        System.out.println(
                String.format("Maximum temperature is %.3f",
                        timeSeriesClient.runQuery(timeSeriesName,
                                QueryOperation.MAX,
                                timeSeriesInfo.getStartDateTime(), timeSeriesInfo.getEndDateTime())));
    }
    
    @Test
    public void timeSeriesSimulationExample() {
        // Initialise the simulator - daily drift is 2%, daily volatility is 5%
        // Implies on average, over the course of a day, the value will increase by 2%
        // and with ~70% probability the series will be between -3% and 7% of its original value
        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(2, 5);
        // Initial value
        double seriesCurrentValue = 10;
        // Time between observations
        int timeBetweenObservations = 30;
        // Ten iterations
        for (int i = 0; i <= 10; i++) {
            System.out.println(String.format("Series value after %d seconds : %.5f", i * timeBetweenObservations, seriesCurrentValue));
            seriesCurrentValue = timeSeriesSimulator.getNextValue(seriesCurrentValue, timeBetweenObservations);
        }
    }
}
