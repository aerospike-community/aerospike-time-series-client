package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

import javax.management.Query;
import java.util.Random;

public class TestUtilities {
    /**
     * Truncate time series data where the time series set name is as per the argument
     * @param timeSeriesSetName
     */
    public static void removeTimeSeriesTestDataForSet(String timeSeriesSetName){
        AerospikeClient asClient = new AerospikeClient(TestConstants.AEROSPIKE_HOST,Constants.DEFAULT_AEROSPIKE_PORT);

        asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, timeSeriesSetName, null);
        asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, TimeSeriesClient.timeSeriesIndexSetName(timeSeriesSetName), null);

    }

    public static int blockCountForTimeseries(TimeSeriesClient timeSeriesClient, String asNamespace,String timeSeriesName) {
        Statement stmt = new Statement();
        stmt.setNamespace(timeSeriesClient.asNamespace);
        stmt.setSetName(timeSeriesClient.getTimeSeriesSet());
        stmt.setBinNames(Constants.METADATA_BIN_NAME);
        stmt.setFilter(Filter.contains(Constants.METADATA_BIN_NAME, IndexCollectionType.MAPVALUES, timeSeriesName));

        RecordSet rs = timeSeriesClient.asClient.query( new QueryPolicy(timeSeriesClient.getReadPolicy()), stmt);
        int blockCount = 0;
        while (rs.next()) blockCount++;
        return blockCount;
    }

    /**
     * Utility constructor where we use the default drift and volatility values
     * @param asHost
     * @param asNamespace
     * @param observationIntervalSeconds
     * @param runDurationSeconds
     * @param accelerationFactor
     * @param threadCount
     * @param timeSeriesCount
     */
    static TimeSeriesBenchmarker realTimeInsertBenchmarker(String asHost, String asNamespace, String asSet, int observationIntervalSeconds, int runDurationSeconds, int accelerationFactor,
                                                           int threadCount, int timeSeriesCount){
        return new TimeSeriesBenchmarker(asHost,asNamespace,asSet, OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,observationIntervalSeconds,
                runDurationSeconds,accelerationFactor,threadCount,timeSeriesCount,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK,0,
                TimeSeriesBenchmarker.DEFAULT_DAILY_DRIFT_PCT, TimeSeriesBenchmarker.DEFAULT_DAILY_VOLATILITY_PCT,new Random().nextLong());
    }

    /**
     * Convenience factory method to allow initiation of a Benchmarker object for test purposes
     * @param asHost
     * @param asNamespace
     * @param observationIntervalSeconds
     * @param timeSeriesRangeSeconds
     * @param threadCount
     * @param timeSeriesCount
     * @return
     */
    static TimeSeriesBenchmarker batchInsertBenchmarker(String asHost, String asNamespace, String asSet, int observationIntervalSeconds, long timeSeriesRangeSeconds,
                                                        int threadCount, int timeSeriesCount, int recordsPerBlock, long randomSeed){
        return new TimeSeriesBenchmarker(asHost,asNamespace,asSet,OptionsHelper.BenchmarkModes.BATCH_INSERT,observationIntervalSeconds,0,0,threadCount,
        timeSeriesCount,recordsPerBlock,timeSeriesRangeSeconds, TimeSeriesBenchmarker.DEFAULT_DAILY_DRIFT_PCT, TimeSeriesBenchmarker.DEFAULT_DAILY_VOLATILITY_PCT,randomSeed);
    }
}
