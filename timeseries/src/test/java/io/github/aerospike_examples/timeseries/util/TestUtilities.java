package io.github.aerospike_examples.timeseries.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;

import java.util.Vector;

public final class TestUtilities {

    public static void removeTimeSeriesTestDataForSet(String timeSeriesSetName) {
        AerospikeClient asClient = new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT);

        asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, timeSeriesSetName, null);
        asClient.truncate(new InfoPolicy(), TestConstants.AEROSPIKE_NAMESPACE, TimeSeriesClient.timeSeriesIndexSetName(timeSeriesSetName), null);
    }

    public static TimeSeriesClient defaultTimeSeriesClient() {
        return new TimeSeriesClient(new AerospikeClient(TestConstants.AEROSPIKE_HOST, Constants.DEFAULT_AEROSPIKE_PORT),
                TestConstants.AEROSPIKE_NAMESPACE, TestConstants.TIME_SERIES_TEST_SET, Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
    }

    public static int blockCountForTimeseries(TimeSeriesClient timeSeriesClient, String timeSeriesName) {
        // Filter expression - only get records where metadata.timeseriesfieldname = timeseriesname
        Exp filterExp =
                Exp.eq(
                        MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(Constants.TIME_SERIES_NAME_FIELD_NAME),
                                Exp.bin(Constants.METADATA_BIN_NAME, Exp.Type.MAP)),
                        Exp.val(timeSeriesName));

        Vector<Record> records = new Vector<>();
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.filterExp = Exp.build(filterExp);
        timeSeriesClient.getAsClient().scanAll(
                scanPolicy, timeSeriesClient.getAsNamespace(), timeSeriesClient.getTimeSeriesSet(),
                // Callback is a lambda function
                (key, record) -> records.add(record));
        return records.size();
    }

}
