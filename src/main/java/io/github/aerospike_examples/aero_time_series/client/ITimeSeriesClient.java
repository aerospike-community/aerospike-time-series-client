package io.github.aerospike_examples.aero_time_series.client;

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
     * Save data points to the database
     * <p>
     * Inserts always go to the current block for the time series which has key TimeSeriesName
     *
     * @param timeSeriesName - time series name
     * @param dataPoints - data points as an array
     */
    void put(String timeSeriesName, DataPoint[] dataPoints);

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

    /**
     * Run a query vs a particular time series range. Query types are as per the enum QueryOperation
     * @param timeSeriesName
     * @param operation
     * @param fromDateTime
     * @param toDateTime
     * @return
     */
    double runQuery(String timeSeriesName, TimeSeriesClient.QueryOperation operation, Date fromDateTime, Date toDateTime);

}
