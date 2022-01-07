package com.aerospike.api.time_series;

import java.util.Date;

public class DataPoint {
    private long timestamp;
    private double value;

    public DataPoint(long timestamp,double value){
        this.timestamp = timestamp;
        this.value = value;
    }

    public DataPoint(Date date, double value){
        this(epochSecondsToTimestamp(date.getTime()),value);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString(){
        return String.format("(%s,%f)",timestamp,value);
    }

    public boolean equals(DataPoint d){
        return timestamp == d.timestamp && value == d.value;
    }

    static long epochSecondsToTimestamp(long epochSeconds){
        return epochSeconds * (long)Math.pow(10,Constants.TIMESTAMP_DECIMAL_PLACES_PER_SECOND);
    }
}
