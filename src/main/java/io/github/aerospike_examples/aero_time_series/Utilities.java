package io.github.aerospike_examples.aero_time_series;

import com.aerospike.client.policy.ScanPolicy;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;

import java.util.Vector;

/**
 * Useful functions
 */
public class Utilities {
    /**
     * Utility function to test a value is within tolerance
     * @param expectedValue expected value
     * @param actualValue actual value
     * @param tolerancePct acceptable tolerance as a percentage
     * @return boolean indicating whether value is within tolerance
     */
    public static boolean valueInTolerance(double expectedValue, double actualValue,double tolerancePct){
        return Math.abs((actualValue - expectedValue)/expectedValue) < tolerancePct / 100;
    }

    /**
     * Get a list of all the time series in the database
     * @param timeSeriesClient object to use
     * @return Vector containing available time series names
     */
    public static Vector<String> getTimeSeriesNames(TimeSeriesClient timeSeriesClient){
        Vector<String> timeSeriesNames = new Vector<>();
        timeSeriesClient.getAsClient().scanAll(
                new ScanPolicy(), timeSeriesClient.getAsNamespace(), timeSeriesClient.timeSeriesIndexSetName(),
                // Callback is a lambda function
                (key, record) -> timeSeriesNames.add(record.getString(Constants.TIME_SERIES_NAME_FIELD_NAME)),
                Constants.TIME_SERIES_NAME_FIELD_NAME);
        return timeSeriesNames;
    }

    /**
     * Throw this error if we have parsing exceptions
     */
    public static class ParseException extends Exception{
        public ParseException(String message){
            super(message);
        }
    }

}
