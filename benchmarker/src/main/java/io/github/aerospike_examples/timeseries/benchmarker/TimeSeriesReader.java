package io.github.aerospike_examples.timeseries.benchmarker;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.util.Constants;
import io.github.aerospike_examples.timeseries.util.Utilities;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import java.util.Vector;

/**
 * Time Series Reader class to write out a time series to the command line
 */
@SuppressWarnings("WeakerAccess") // Want to expose class
public class TimeSeriesReader {

    // Specify Aerospike cluster, namespace, set
    private final AerospikeClient asClient;
    private final String asNamespace;
    private final String asSet;
    private String timeSeriesName;

    private TimeSeriesReader(AerospikeClient asClient, String asNamespace, String asSet, String timeSeriesName) {
        this.asClient = asClient;
        this.asNamespace = asNamespace;
        this.asSet = asSet;
        this.timeSeriesName = timeSeriesName;
    }

    /**
     * Entry point for command line use of the time series reader
     *
     * @param args command line arguments as String[]
     */
    public static void main(String[] args) {
        try {
            TimeSeriesReader timeSeriesReader = initBenchmarkerFromStringArgs(args);
            timeSeriesReader.run();
        } catch (Utilities.ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TimeSeriesReader", OptionsHelper.cmdLineOptionsForReader());
        }
    }

    /**
     * Helper method allowing a TimeSeriesBenchmarker to be initialised from an array of Strings - as per main method
     * Protected visibility to allow testing use
     */
    private static TimeSeriesReader initBenchmarkerFromStringArgs(String[] args) throws Utilities.ParseException {
        TimeSeriesReader timeSeriesReader;
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(OptionsHelper.cmdLineOptionsForReader(), args);

            timeSeriesReader = new TimeSeriesReader(
                    new AerospikeClient(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.HOST_FLAG), Constants.DEFAULT_AEROSPIKE_PORT),
                    OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG),
                    OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG),
                    OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.TIME_SERIES_NAME_FLAG)
            );
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TimeSeriesReader", OptionsHelper.cmdLineOptionsForReader());
            throw (new Utilities.ParseException(e.getMessage()));
        }
        return timeSeriesReader;
    }

    private void run() {
        System.out.println("Running TimeSeriesReader\n");
        TimeSeriesClient timeSeriesClient = new TimeSeriesClient(asClient, asNamespace, asSet,
                Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);

        if (timeSeriesName != null) {
            System.out.println(String.format("Running time series reader for %s", timeSeriesName));
        } else {
            Vector<String> timeSeriesNames = Utilities.getTimeSeriesNames(timeSeriesClient);
            if (timeSeriesNames.size() > 0) {
                timeSeriesName = timeSeriesNames.get(0);
                System.out.println(String.format("No time series specified - selecting series %s\n", timeSeriesName));
            } else {
                System.out.println(String.format("No time series data found in %s.%s\n", asNamespace, asSet));
            }
        }
        if (timeSeriesName != null) {
            timeSeriesClient.printTimeSeries(timeSeriesName);
        }
    }

}
