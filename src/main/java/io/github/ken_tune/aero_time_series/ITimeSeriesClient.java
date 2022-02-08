package io.github.ken_tune.aero_time_series;

import java.util.Date;

public interface ITimeSeriesClient {
    /**
     * Put a data point for time series timeSeriesName into the database
     * A datapoint is a structure containing a timestamp and a floating point value
     *
     * @param timeSeriesName
     * @param dataPoint
     */
    void put(String timeSeriesName,DataPoint dataPoint);

    /**
     * Saves data point to the database
     * <p>
     * By default insert always goes to the current block for the time series which has key TimeSeriesName
     * <p>
     * If Max Values per block is exceeded, save block under name TimeSeries-StartTime and remove 'current' block
     * Note that put(String,DataPoint) is the same as this call, but sets maxEntryCount to a default value, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK
     *
     * @param timeSeriesName
     * @param dataPoint
     * @param maxEntryCount
     */
    void put(String timeSeriesName, DataPoint dataPoint, int maxEntryCount);

    /**
     * Save data points to the database
     * <p>
     * Inserts always go to the current block for the time series which has key TimeSeriesName
     *
     * @param timeSeriesName - time series name
     * @param dataPoints - data points as an array
     */
    void put(String timeSeriesName, DataPoint[] dataPoints);

    /**
     * Save data points to the database
     * <p>
     * Inserts always go to the current block for the time series which has key TimeSeriesName
     *
     * @param timeSeriesName - time series name
     * @param dataPoints - data points as an array
     * @param maxEntryCount - max number of data points to keep in a block
     */
    void put(String timeSeriesName, DataPoint[] dataPoints, int maxEntryCount);

    /**
     * Get all the data points for time series timeSeriesName
     * between startDateTime and endDateTime (inclusive)
     *
     *
     * @param timeSeriesName
     * @param startDateTime
     * @param endDateTime
     *
     */
    DataPoint[] getPoints(String timeSeriesName,Date startDateTime, Date endDateTime);

    /**
     * Get a particular data point for timeSeriesName
     * Returns null if no point available
     *
     * @param timeSeriesName
     * @param dateTime
     *
     */
    DataPoint getPoint(String timeSeriesName,Date dateTime);
}
