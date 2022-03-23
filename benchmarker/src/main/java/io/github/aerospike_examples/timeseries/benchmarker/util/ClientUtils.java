package io.github.aerospike_examples.timeseries.benchmarker.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.WritePolicy;
import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;
import io.github.aerospike_examples.timeseries.TimeSeriesInfo;
import io.github.aerospike_examples.timeseries.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class ClientUtils {

    private ClientUtils() {
    }

    /**
     * Utility method to remove 'dummy' records
     * These are inserted by the benchmarker at the start of a run to prevent all the blocks filling up at the same time
     * After the run, the dummy records are removed using this function
     *
     * @param timeSeriesName Time Series to remove dummy records for
     */
    public static void removeDummyRecords(TimeSeriesClient timeSeriesClient, String timeSeriesName) {
        long startTime;
        // Create a policy to make sure, when we look up the first start time from the index that we don't get an error if it doesn't exist
        WritePolicy blockRecordExistsPolicy = new WritePolicy(timeSeriesClient.getWritePolicy());
        // Now try and get the earliest start time from the index
        blockRecordExistsPolicy.filterExp = Exp.build(Exp.binExists(Constants.TIME_SERIES_INDEX_BIN_NAME));

        Record startTimeFromFirstHistoricBlockRecord = timeSeriesClient.getAsClient().operate(blockRecordExistsPolicy,
                timeSeriesClient.asKeyForTimeSeriesIndexes(timeSeriesName),
                MapOperation.getByIndex(Constants.TIME_SERIES_INDEX_BIN_NAME, 0, MapReturnType.KEY));

        // If there are historic blocks
        if (startTimeFromFirstHistoricBlockRecord != null) {
            startTime = startTimeFromFirstHistoricBlockRecord.getLong(Constants.TIME_SERIES_INDEX_BIN_NAME);
            // Remove dummy records from the first block
            Record r = timeSeriesClient.getAsClient().operate(timeSeriesClient.getWritePolicy(),
                    timeSeriesClient.asKeyForHistoricTimeSeriesBlock(timeSeriesName, startTime),
                    MapOperation.removeByKeyRange(Constants.TIME_SERIES_BIN_NAME, null, new Value.IntegerValue(1), MapReturnType.NONE),
                    MapOperation.size(Constants.TIME_SERIES_BIN_NAME)
            );
            // The resulting entry count is returned
            long entryCount = (Long) (r.getList(Constants.TIME_SERIES_BIN_NAME).get(1));

            timeSeriesClient.getAsClient().operate(blockRecordExistsPolicy, timeSeriesClient.asKeyForTimeSeriesIndexes(timeSeriesName),
                    MapOperation.put(new MapPolicy(), Constants.TIME_SERIES_INDEX_BIN_NAME,
                            new Value.StringValue(Constants.ENTRY_COUNT_FIELD_NAME), new Value.LongValue(entryCount),
                            CTX.mapIndex(0)
                    )
            );
        }
        // Remove dummy records from the current block if it exists
        // Turns out we need to do this in a try/catch as can't avoid 'key not found' if not found
        try {
            timeSeriesClient.getAsClient().operate(timeSeriesClient.getWritePolicy(), timeSeriesClient.asCurrentKeyForTimeSeries(timeSeriesName),
                    MapOperation.removeByKeyRange(Constants.TIME_SERIES_BIN_NAME, null, new Value.IntegerValue(1), MapReturnType.NONE)
            );
        } catch (AerospikeException e) {
            //noinspection StatementWithEmptyBody - deliberate
            if (e.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                /*do nothing*/
                /* This is OK  - record may not exist */
            } else {
                throw e;
            }
        }
    }

    /**
     * Utility method to print out a time series
     *
     * @param timeSeriesName Name of time series to print data for
     */
    public static void printTimeSeries(TimeSeriesClient timeSeriesClient, String timeSeriesName) {
        String timeSeriesDateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        SimpleDateFormat dateFormatter = new SimpleDateFormat(timeSeriesDateFormat);

        TimeSeriesInfo timeSeriesInfo = TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient, timeSeriesName);
        System.out.println(timeSeriesInfo);
        System.out.println();
        DataPoint[] dataPoints = timeSeriesClient.getPoints(timeSeriesName, new Date(timeSeriesInfo.getStartDateTimestamp()),
                new Date(timeSeriesInfo.getEndDateTimestamp()));
        System.out.println("Timestamp,Value");
        for (DataPoint dataPoint : dataPoints) {
            System.out.printf("%s,%.5f%n", dateFormatter.format(new Date(dataPoint.getTimestamp())), dataPoint.getValue());
        }
    }
}
