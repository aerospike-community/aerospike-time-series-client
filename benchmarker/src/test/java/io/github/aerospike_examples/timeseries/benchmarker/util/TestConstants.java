package io.github.aerospike_examples.timeseries.benchmarker.util;

@SuppressWarnings("ALL")
public final class TestConstants {

    public static String AEROSPIKE_HOST = "localhost";
    public static String AEROSPIKE_NAMESPACE = "test";

    public static String TIME_SERIES_TEST_SET = "TimeSeriesTest";

    // Seed for randomiser so we can acheive deterministic results in testing
    public static final long RANDOM_SEED = 6760187239798559903L;

    // This time series name is the first one that gets generated if using the seed 6760187239798559903L
    public static String REFERENCE_TIME_SERIES_NAME = "LSVLRSBQIZ";
}
