package io.github.aerospike_examples.aero_time_series.client;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Container object for key information about a single time series
 */
public class TimeSeriesInfo {
    private String seriesName;
    private long startDateTime;
    private long endDateTime;
    private long dataPointCount;

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(DATE_FORMAT);

    /**
     * TimeSeriesInfo constructor object
     * @param seriesName - name of time series
     * @param startDateTime - earliest data point timestamp for series
     * @param endDateTime - latest data point timestamp for series
     * @param dataPointCount - number of points in the time series
     */
    public TimeSeriesInfo(String seriesName, long startDateTime, long endDateTime, long dataPointCount){
        this.seriesName = seriesName;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.dataPointCount = dataPointCount;
    }

    /**
     * Get time series name
     * @return time series name
     */
    public String getSeriesName() {
        return seriesName;
    }

    /**
     * Timestamp of earliest time series point
     * @return earliest data point timestamp
     */
    public long getStartDateTime() {
        return startDateTime;
    }

    /**
     * Timestamp of latest time series point
     * @return latest data point timestamp
     */
    public long getEndDateTime() {
        return endDateTime;
    }

    /**
     * Number of data points in series
     * @return number of data points in series
     */
    public long getDataPointCount() {
        return dataPointCount;
    }

    public String toString(){
        return String.format("Name : %s Start Date : %s End Date %s Data point count : %s",
                seriesName,DATE_FORMATTER.format(new Date(startDateTime)),DATE_FORMATTER.format(new Date(endDateTime)),dataPointCount);
    }

    /**
     * Static method allowing generation of a TimeSeriesInfo object
     * @param timeSeriesClient client object
     * @param timeSeriesName time series name
     * @return
     */
    public static TimeSeriesInfo getTimeSeriesDetails(TimeSeriesClient timeSeriesClient, String timeSeriesName){
        long startDateTime = timeSeriesClient.startTimeForSeries(timeSeriesName);
        long endDateTime = timeSeriesClient.endTimeForSeries(timeSeriesName);
        long dataPointCount = timeSeriesClient.dataPointCount(timeSeriesName);
        return new TimeSeriesInfo(timeSeriesName,startDateTime,endDateTime,dataPointCount);
    }

}
