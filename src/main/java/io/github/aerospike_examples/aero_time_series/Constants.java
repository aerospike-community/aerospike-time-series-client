package io.github.aerospike_examples.aero_time_series;

public class Constants {
    /**
     * Default Aerospike port
     */
    public final static int DEFAULT_AEROSPIKE_PORT = 3000;

    /**
     * Upper limit for single key updates is ~100. We use this value to flag benchmark loads that may exceed this
     */
    public final static int SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC = 100;

    /**
     * Default set for storing time series data - default is set=TimeSeries
     */
    public final static String DEFAULT_TIME_SERIES_SET = "TimeSeries";

    /**
     * Bin in which we store the time series data - bin = tsSeries
     */
    public final static String TIME_SERIES_BIN_NAME = "tsSeries";

    /**
     * Bin in which we store time series index data - bin = tsIndex
     */
    public final static String TIME_SERIES_INDEX_BIN_NAME = "tsIndex";

    /**
     * Field name used when storing time series name - = TimeSeriesName
     */
    public final static String TIME_SERIES_NAME_FIELD_NAME = "TimeSeriesName";

    /**
     * Field name used when storing block start time - = StartTime
     */
    public final static String START_TIME_FIELD_NAME = "StartTime";

    /**
     * Field name used when storing block end time - = EndTime
     */
    public final static String END_TIME_FIELD_NAME = "EndTime";

    /**
     * Field name used when storing metadata - = metadata
     */
    public final static String METADATA_BIN_NAME = "Metadata";

    /**
     * Field name used when storing block entry count - = Entrycount
     */
    @SuppressWarnings("SpellCheckingInspection")
    public final static String ENTRY_COUNT_FIELD_NAME = "EntryCount";

    /**
     * When we store the max block entries in force, store under this field name - = maxTSEntries
     */
    public final static String MAX_BLOCK_TIME_SERIES_ENTRIES_FIELD_NAME = "maxTSEntries";

    /**
     * Default value for time series entries per block - = 1000
     */
    public final static int DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK = 1000;

    /**
     * Converting seconds to milliseconds and back again is so prevalent, best to make it a constant for clarity
     */
    public final static long MILLISECONDS_IN_SECOND = 1000;
}
