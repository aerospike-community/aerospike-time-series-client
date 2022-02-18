package io.github.aerospike_examples.aero_time_series.benchmark;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.TestConstants;
import io.github.aerospike_examples.aero_time_series.TestUtilities;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.client.DataPoint;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesClient;
import io.github.aerospike_examples.aero_time_series.client.TimeSeriesInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Vector;

public class QueryBenchmarkerTest {
    /**
     Check acceleration flag presence triggers an error if found in query invocation
     */
    @Test
    public void accelerationFlagHandled() throws IOException {
        int accelerationFactor = 5;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,accelerationFactor);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
        Check timeSeriesCount flag presence triggers an error if found in query invocation
     */
    @Test
    public void timeSeriesCountFlagHandled() throws IOException {
        int timeSeriesCount = 10;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,timeSeriesCount);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
        Check intervalBetweenObservations flag presence triggers an error if found in query invocation
     */
    @Test
    public void intervalBetweenObservationsFlagHandled() throws IOException {
        int intervalBetweenObservations = 10;
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%d",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,intervalBetweenObservations);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
        Check timeSeriesRange flag presence triggers an error if found in query invocation
     */
    @Test
    public void timeSeriesRangeFlagHandled() throws IOException {
        String timeSeriesRange = String.format("%d%s",1, OptionsHelper.TimeUnitIndicators.DAY);
        int runDuration = 1;

        // Create the string argument array
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%s -%s %%d -%s %%s",
                OptionsHelper.BenchmarkerFlags.HOST_FLAG, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG,OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG,
                OptionsHelper.BenchmarkerFlags.MODE_FLAG, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG, OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);

        String commandLineArguments =
                String.format(formatString, TestConstants.AEROSPIKE_HOST, TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,OptionsHelper.BenchmarkModes.QUERY,
                        runDuration,timeSeriesRange);

        Vector<String> consoleOutput = TestUtilities.runBenchmarkerGetOutput(commandLineArguments);

        Assert.assertEquals(consoleOutput.get(0), String.format("-%s flag (%s) should not be used in %s mode", OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,
                OptionsHelper.standardCmdLineOptions().getOption(OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).getLongOpt(),
                OptionsHelper.BenchmarkModes.QUERY));
    }

    /**
     * Check a good run runs without errors
     * @throws Exception Can throw exception
     */
    @Test
    public void goodRun() throws Exception{
        TestUtilities.removeTimeSeriesTestDataForSet(TestConstants.TIME_SERIES_TEST_SET);

        int intervalBetweenUpdates = 10;
        int threadCount = 10;
        int timeSeriesCount = 10;
        int recordsPerBlock = 1000;
        long timeSeriesRangeSeconds = 86400 * 10;
        int queryRunDuratinoSeconds = 10;

        // Keep track of the start time - useful when we retrieve the data points
        long startTime = System.currentTimeMillis();

        TimeSeriesBenchmarker benchmarker = TestUtilities.batchInsertBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,
                TestConstants.TIME_SERIES_TEST_SET,intervalBetweenUpdates,timeSeriesRangeSeconds,
                threadCount,timeSeriesCount,recordsPerBlock, TestConstants.RANDOM_SEED);

        benchmarker.run();

        TimeSeriesBenchmarker readBenchmarker = TestUtilities.queryBenchmarker(TestConstants.AEROSPIKE_HOST,TestConstants.AEROSPIKE_NAMESPACE,TestConstants.TIME_SERIES_TEST_SET,
                queryRunDuratinoSeconds,threadCount,TestConstants.RANDOM_SEED);
        readBenchmarker.run();

    }

    @Test
    public void scratch(){
        AerospikeClient asClient = new AerospikeClient(TestConstants.AEROSPIKE_HOST,3000);
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(asClient,TestConstants.AEROSPIKE_NAMESPACE,"TimeSeriesSet2",
                Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
//        String timeSeriesName = "ZHROKZDMKZ";
//        Record r = asClient.get(timeSeriesClient.getReadPolicy(),timeSeriesClient.asCurrentKeyForTimeSeries(timeSeriesName));
//        System.out.println(r);
//        Record s = asClient.get(timeSeriesClient.getReadPolicy(),timeSeriesClient.asKeyForTimeSeriesIndexes(timeSeriesName));
//        System.out.println(s);
        Vector<String> timeSeriesNames = Utilities.getTimeSeriesNames(timeSeriesClient);
        for(String timeSeriesName:timeSeriesNames)
        {
            System.out.println(timeSeriesName);
            TimeSeriesInfo timeSeriesInfo = TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient,timeSeriesName);
            System.out.println(timeSeriesInfo);
            DataPoint[] dataPoints = timeSeriesClient.getPoints(timeSeriesName,new Date(timeSeriesInfo.getStartDateTime()),new Date(timeSeriesInfo.getEndDateTime()));
            for(DataPoint dataPoint:dataPoints){
                System.out.println(String.format("%s : %.5f",new Date(dataPoint.getTimestamp()),dataPoint.getValue()));
            }

        }




    }
}
