package com.aerospike.api.time_series;

import com.aerospike.client.policy.WritePolicy;

public class Constants {
    public final static int DEFAULT_AEROSPIKE_PORT = 3000;

    /* Accessible within package */
    final static String AS_TIME_SERIES_SET = "TS";
    final static WritePolicy DEFAULT_WRITE_POLICY = new WritePolicy();
    final static String TIME_SERIES_BIN_NAME = "tSeries";
    final static String TIMESTAMP_BIN_NAME = "Timestamp";
    final static String VALUE_BIN_NAME="Value";

    final static int TIMESTAMP_DECIMAL_PLACES_PER_SECOND = 3;

}
