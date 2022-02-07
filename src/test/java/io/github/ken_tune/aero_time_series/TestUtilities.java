package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

import java.util.Random;

public class TestUtilities {
    public static int blockCountForTimeseries(AerospikeClient asClient, String asNamespace,String timeSeriesName) {
        Statement stmt = new Statement();
        stmt.setNamespace(asNamespace);
        stmt.setSetName(Constants.AS_TIME_SERIES_SET);
        stmt.setBinNames(Constants.METADATA_BIN_NAME);
        stmt.setFilter(Filter.contains(Constants.METADATA_BIN_NAME, IndexCollectionType.MAPVALUES, timeSeriesName));

        RecordSet rs = asClient.query(null, stmt);
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
    static TimeSeriesBenchmarker realTimeInsertBenchmarker(String asHost, String asNamespace, int observationIntervalSeconds, int runDurationSeconds, int accelerationFactor,
                                                           int threadCount, int timeSeriesCount){
        return new TimeSeriesBenchmarker(asHost,asNamespace,OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,observationIntervalSeconds,
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
    static TimeSeriesBenchmarker batchInsertBenchmarker(String asHost, String asNamespace, int observationIntervalSeconds, int timeSeriesRangeSeconds,
                                                        int threadCount, int timeSeriesCount, int recordsPerBlock, long randomSeed){
        return new TimeSeriesBenchmarker(asHost,asNamespace,OptionsHelper.BenchmarkModes.BATCH_INSERT,observationIntervalSeconds,0,0,threadCount,
        timeSeriesCount,recordsPerBlock,timeSeriesRangeSeconds, TimeSeriesBenchmarker.DEFAULT_DAILY_DRIFT_PCT, TimeSeriesBenchmarker.DEFAULT_DAILY_VOLATILITY_PCT,randomSeed);
    }
}
