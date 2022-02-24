package io.github.aerospike_examples.aero_time_series;

import com.aerospike.client.policy.ScanPolicy;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;

import java.util.Date;
import java.util.HashSet;
import java.util.Vector;

/**
 * Useful functions
 */
public class Utilities {
    /**
     * Class has static methods only, so hide constructor
     */
    private Utilities(){}
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
        HashSet<String> timeSeriesNames = new HashSet<>();
        timeSeriesClient.getAsClient().scanAll(
                new ScanPolicy(), timeSeriesClient.getAsNamespace(), timeSeriesClient.timeSeriesIndexSetName(),
                // Callback is a lambda function
                (key, record) -> timeSeriesNames.add(record.getString(Constants.TIME_SERIES_NAME_FIELD_NAME)),
                Constants.TIME_SERIES_NAME_FIELD_NAME);
        // Need to check for series which haven't yet been indexed if we haven't got any time series yet
        if(timeSeriesNames.size() == 0) {
            timeSeriesClient.getAsClient().scanAll(
                    new ScanPolicy(), timeSeriesClient.getAsNamespace(), timeSeriesClient.getTimeSeriesSet(),
                    // Callback is a lambda function
                    (key, record) -> {
                        String timeSeriesName = (String) (record.getMap(Constants.METADATA_BIN_NAME).get(Constants.TIME_SERIES_NAME_FIELD_NAME));
                        timeSeriesNames.add(timeSeriesName);
                    },
                    Constants.METADATA_BIN_NAME);
        }
        return new Vector<>(timeSeriesNames);
    }

    /**
     * Throw this error if we have parsing exceptions
     */
    public static class ParseException extends Exception{
        /**
         * Parse Exception constructor
         * @param message error message
         */
        public ParseException(String message){
            super(message);
        }
    }

    /**
     * Return truncated timestamp i.e. timestamp with the time component removed
     * @param timestamp as long
     * @return truncated timestamp
     */
    public static long getTruncatedTimestamp(long timestamp){
        return timestamp - timestamp % (24 * 60 * 60 * Constants.MILLISECONDS_IN_SECOND);
    }

    /**
     *
     * @param timestamp date to truncate
     * @return truncated date
     */
    @SuppressWarnings("unused") // method deliberately exposed
    public static Date getTruncatedTimestamp(Date timestamp){
        return new Date(getTruncatedTimestamp(timestamp.getTime()));
    }

    /**
     * Increment a date by a specified number of milliseconds
     * @param timestamp date/time to increment
     * @param incrementInMs magnitude of increment in milliseconds
     * @return incremented date
     */
    @SuppressWarnings("WeakerAccess") // public access required
    public static Date incrementDateUsingMs(Date timestamp, long incrementInMs){
        return new Date(timestamp.getTime() + incrementInMs);
    }

    /**
     * Increment a date by a specified number of seconds
     * @param timestamp date/time to increment
     * @param incrementInSeconds magnitude of increment in seconds
     * @return incremented date
     */
    public static Date incrementDateUsingSeconds(Date timestamp,long incrementInSeconds){
        return incrementDateUsingMs(timestamp, incrementInSeconds * Constants.MILLISECONDS_IN_SECOND);
    }
}
