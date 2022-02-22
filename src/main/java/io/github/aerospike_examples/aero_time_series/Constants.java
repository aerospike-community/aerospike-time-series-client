package io.github.aerospike_examples.aero_time_series;

public class Constants {
    public final static int DEFAULT_AEROSPIKE_PORT = 3000;
    public final static int SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC = 100;

    /* Accessible within package */
    public final static String DEFAULT_TIME_SERIES_SET = "TimeSeries";

    public final static String TIME_SERIES_BIN_NAME = "tsSeries";
    public final static String TIME_SERIES_INDEX_BIN_NAME = "tsIndex";
    public final static String TIME_SERIES_NAME_FIELD_NAME = "TimeSeriesName";
    public final static String START_TIME_FIELD_NAME = "StartTime";
    public final static String END_TIME_FIELD_NAME = "EndTime";
    public final static String METADATA_BIN_NAME = "Metadata";
    public final static String ENTRY_COUNT_FIELD_NAME = "EntryCount";
    public final static String MAX_BLOCK_TIME_SERIES_ENTRIES_FIELD_NAME = "maxTSEntries";

    public final static int DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK = 1000;

    // For the avoidance of doubt and clarity
    // Type everything as longs - we can get into trouble with overflow when multiplying if not
    public final static long MILLISECONDS_IN_SECOND = 1000;
}
