package io.github.ken_tune.time_series;

import com.aerospike.client.policy.WritePolicy;

public class Constants {
    public final static int DEFAULT_AEROSPIKE_PORT = 3000;

    /* Accessible within package */
    final static String AS_TIME_SERIES_SET = "TS";
    final static String AS_TIME_SERIES_INDEX_SET = "TimeSeriesIndexes";
    final static WritePolicy DEFAULT_WRITE_POLICY = new WritePolicy();
    final static String TIME_SERIES_BIN_NAME = "tsSeries";
    final static String TIME_SERIES_INDEX_BIN_NAME = "tsIndex";
    final static String TIME_SERIES_NAME_FIELD_NAME = "TimeSeriesName";
    final static String START_TIME_FIELD_NAME = "StartTime";
    final static String METADATA_BIN_NAME = "Metadata";
    final static String MAX_BLOCK_TIME_SERIES_ENTRIES_FIELD_NAME = "maxTSEntries";

    final static int TIMESTAMP_DECIMAL_PLACES_PER_SECOND = 3;
    final static int DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK = 1000;
}
