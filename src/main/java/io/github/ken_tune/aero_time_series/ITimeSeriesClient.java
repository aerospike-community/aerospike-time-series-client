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
