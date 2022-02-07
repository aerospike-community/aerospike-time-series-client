package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

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
}
