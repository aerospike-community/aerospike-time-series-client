package io.github.aerospike_examples.aero_time_series.client;

import java.util.Date;

@SuppressWarnings("unused")
public interface ITimeSeriesClient {
    /**
     * Put a data point for time series timeSeriesName into the database
     * A data point is a structure containing a timestamp and a floating point value
     *
     * @param timeSeriesName - name of time series
     * @param dataPoint - data point
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
     * @param timeSeriesName - time series name
     * @param startDateTime - start time for interval
     * @param endDateTime - end time for interval
     *
     */
    DataPoint[] getPoints(String timeSeriesName,Date startDateTime, Date endDateTime);

    /**
     * Get a particular data point for timeSeriesName
     * Returns null if no point available
     *
     * @param timeSeriesName - time series name
     * @param dateTime - date time to look up data point for
     *
     */
    DataPoint getPoint(String timeSeriesName,Date dateTime);

    /**
     * Run a query vs a particular time series range. Query types are as per the enum QueryOperation
     * @param timeSeriesName - time series name
     * @param operation - operation to apply vs query e.g. count, avg, min, max, vol
     * @param fromDateTime - start time for relevant time range
     * @param toDateTime - end time for relevant time range
     * @return double resulting from query
     */
    double runQuery(String timeSeriesName, TimeSeriesClient.QueryOperation operation, Date fromDateTime, Date toDateTime);

}
