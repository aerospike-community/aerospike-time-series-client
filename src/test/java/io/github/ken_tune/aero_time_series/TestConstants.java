package io.github.ken_tune.aero_time_series;

public class TestConstants {
    public static String AEROSPIKE_HOST="172.28.128.7";
    public static String AEROSPIKE_NAMESPACE="test";

    public static String TIME_SERIES_TEST_SET = "TimeSeriesTest";


    // Seed for randomiser so we can acheive deterministic results in testing
    static final long RANDOM_SEED = 6760187239798559903L;

    // This time series name is the first one that gets generated if using the seed 6760187239798559903L
    public static String REFERENCE_TIME_SERIES_NAME="LSVLRSBQIZ";

}
