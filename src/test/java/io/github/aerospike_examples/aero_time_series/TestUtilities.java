package io.github.aerospike_examples.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import io.github.aerospike_examples.aero_time_series.benchmark.OptionsHelper;
import io.github.aerospike_examples.aero_time_series.benchmark.TimeSeriesBenchmarker;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;

import java.util.Random;
import java.util.Vector;

public class TestUtilities {
    /**
     * Truncate time series data where the time series set name is as per the argument
     * @param timeSeriesSetName - name of the set time series is being stored in
     */
    public static void removeTimeSeriesTestDataForSet(String timeSeriesSetName){
        AerospikeClient asClient = new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT);

        asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, timeSeriesSetName, null);
        asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, TimeSeriesClient.timeSeriesIndexSetName(timeSeriesSetName), null);

    }

    /**
     * Count the number of blocks for a time series - used by test functions
     * @param timeSeriesClient - time series client
     * @param timeSeriesName - name of time series
     * @return number of blocks
     */
    public static int blockCountForTimeseries(TimeSeriesClient timeSeriesClient, String timeSeriesName) {
        Statement stmt = new Statement();
        stmt.setNamespace(timeSeriesClient.getAsNamespace());
        stmt.setSetName(timeSeriesClient.getTimeSeriesSet());
        stmt.setBinNames(Constants.METADATA_BIN_NAME);
        stmt.setFilter(Filter.contains(Constants.METADATA_BIN_NAME, IndexCollectionType.MAPVALUES, timeSeriesName));

        RecordSet rs = timeSeriesClient.getAsClient().query( new QueryPolicy(timeSeriesClient.getReadPolicy()), stmt);
        int blockCount = 0;
        while (rs.next()) blockCount++;
        return blockCount;
    }

    /**
     * Utility constructor where we use the default drift and volatility values
     * @param asHost - seed host for Aerospike database
     * @param asNamespace - namespace to write data into
     * @param observationIntervalSeconds - time between observations
     * @param runDurationSeconds - total run time
     * @param accelerationFactor - acceleration factor - 'speed up' time
     * @param threadCount - number of threads
     * @param timeSeriesCount - number of time series to generate
     */
    public static TimeSeriesBenchmarker realTimeInsertBenchmarker(String asHost, String asNamespace, String asSet, int observationIntervalSeconds, int runDurationSeconds, int accelerationFactor,
                                                           int threadCount, int timeSeriesCount){
        return new TimeSeriesBenchmarker(asHost,asNamespace,asSet, OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,observationIntervalSeconds,
                runDurationSeconds,accelerationFactor,threadCount,timeSeriesCount,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK,0,
                TimeSeriesBenchmarker.DEFAULT_DAILY_DRIFT_PCT, TimeSeriesBenchmarker.DEFAULT_DAILY_VOLATILITY_PCT,new Random().nextLong());
    }

    /**
     * Convenience factory method to allow initiation of a Benchmarker object for test purposes
     * @param asHost - seed host for Aerospike database
     * @param asNamespace - namespace to write data into
     * @param observationIntervalSeconds - time between observations
     * @param timeSeriesRangeSeconds - period time series generation should cover in seconds
     * @param threadCount - number of threads
     * @param timeSeriesCount - number of time series to generate
     * @return Benchmarker object configured for batch inserts
     */
    public static TimeSeriesBenchmarker batchInsertBenchmarker(String asHost, String asNamespace, String asSet, int observationIntervalSeconds, long timeSeriesRangeSeconds,
                                                        int threadCount, int timeSeriesCount, int recordsPerBlock, long randomSeed){
        return new TimeSeriesBenchmarker(asHost,asNamespace,asSet,OptionsHelper.BenchmarkModes.BATCH_INSERT,observationIntervalSeconds,0,0,threadCount,
        timeSeriesCount,recordsPerBlock,timeSeriesRangeSeconds, TimeSeriesBenchmarker.DEFAULT_DAILY_DRIFT_PCT, TimeSeriesBenchmarker.DEFAULT_DAILY_VOLATILITY_PCT,randomSeed);
    }

    /**
     * Get a list of all the time series in the database
     * @param timeSeriesClient object to use
     * @return Vector containing available time series names
     */
    public static Vector<String> getTimeSeriesNames(TimeSeriesClient timeSeriesClient){
        Vector<String> timeSeriesNames = new Vector<>();
        timeSeriesClient.getAsClient().scanAll(
                new ScanPolicy(), TestConstants.AEROSPIKE_NAMESPACE, timeSeriesClient.timeSeriesIndexSetName(),
                // Callback is a lambda function
                (key, record) -> timeSeriesNames.add(record.getString(Constants.TIME_SERIES_NAME_FIELD_NAME)),
                Constants.TIME_SERIES_NAME_FIELD_NAME);
        return timeSeriesNames;
    }

    // Default TimeSeriesClient for tests
    public static TimeSeriesClient defaultTimeSeriesClient(){
        return new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
    }
}
