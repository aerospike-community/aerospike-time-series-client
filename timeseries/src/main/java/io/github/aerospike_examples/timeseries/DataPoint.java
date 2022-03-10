package io.github.aerospike_examples.timeseries;

import java.util.Date;

/**
 * Fundamental time series quantity is the data point
 * <p>Think of this as an observation</p>
 * <p>It is composed of the time of the observation and the value of the observation</p>
 */
public class DataPoint {

    private final long timestamp;
    private final double value;

    /**
     * Datapoint constructor
     *
     * @param timestamp - timestamp as a long
     * @param value     - data point value
     */
    public DataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * Datapoint constructor
     *
     * @param dateTime - timestamp as a date time value
     * @param value    - data point value
     */
    public DataPoint(Date dateTime, double value) {
        this(dateTime.getTime(), value);
    }

    /**
     * Get the timestamp as a long
     *
     * @return timestamp as long
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get the timestamp as a date/time
     *
     * @return timestamp as date/time
     */
    @SuppressWarnings("unused") // required for API
    public Date getTimestampAsDateTime() {
        return new Date(timestamp);
    }

    /**
     * Get data point value as a double
     *
     * @return value of data point
     */
    public double getValue() {
        return value;
    }

    /**
     * Utility method to present data point as string
     *
     * @return data point as a string
     */
    @Override
    public String toString() {
        return String.format("(%s,%f)", timestamp, value);
    }

    /**
     * Utility method to check if two data points are equal
     *
     * @param dataPoint to check against
     * @return boolean - does this == dataPoint
     */
    public boolean equals(DataPoint dataPoint) {
        return timestamp == dataPoint.timestamp && value == dataPoint.value;
    }
}
