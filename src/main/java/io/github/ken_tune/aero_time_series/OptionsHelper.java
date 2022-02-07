package io.github.ken_tune.aero_time_series;

import org.apache.commons.cli.*;

/**
 * Help with command line processing
 */
public class OptionsHelper {
    /**
     * Flags used at the command line
     */
    static class BenchmarkerFlags{
        static final String HOST_FLAG = "h";
        static final String NAMESPACE_FLAG = "n";
        static final String MODE_FLAG = "m";
        static final String RUN_DURATION_FLAG = "d";
        static final String ACCELERATION_FLAG = "a";
        static final String RECORDS_PER_BLOCK_FLAG = "b";
        static final String THREAD_COUNT_FLAG = "z";
        static final String TIME_SERIES_COUNT_FLAG = "c";
        static final String INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG = "p";
        static final String TIME_SERIES_RANGE_FLAG = "r";
    }

    static class BenchmarkModes{
        static final String REAL_TIME_INSERT = "realTimeWrite";
        static final String BATCH_INSERT = "batchInsert";
        static final String QUERY = "query";
    }
    
    /**
     * Command line options for Main class
     * @return cmdLineOptions
     */
    static Options cmdLineOptions(){
        Options cmdLineOptions = new Options();

        Option hostOption = new Option(BenchmarkerFlags.HOST_FLAG,"host",true,"Aerospike seed host");
        Option namespaceOption = new Option(BenchmarkerFlags.NAMESPACE_FLAG,"namespace",true,"Namespace");
        Option modeOption = new Option(BenchmarkerFlags.MODE_FLAG,"mode",true,
                String.format("Benchmark mode - values allowed are %s and %s",BenchmarkModes.REAL_TIME_INSERT,BenchmarkModes.BATCH_INSERT));
        Option runDurationOption = new Option(BenchmarkerFlags.RUN_DURATION_FLAG,"duration",true,
                String.format("Simulation duration in seconds. Only valid in %s mode.",BenchmarkModes.REAL_TIME_INSERT));
        Option accelerationOption = new Option(BenchmarkerFlags.ACCELERATION_FLAG,"acceleration",true,
                String.format("Simulation acceleration factor (clock speed multiplier). Only valid in %s mode.",BenchmarkModes.REAL_TIME_INSERT));
        Option recordsPerBlockOption = new Option(BenchmarkerFlags.RECORDS_PER_BLOCK_FLAG,"recordsPerBlock",true,
                "Max time series points in each Aerospike object");
        Option timeSeriesRangeOption = new Option(BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,"timeSeriesRange",true,
                String.format("period to be spanned by time series. Only valid in %s mode",BenchmarkModes.BATCH_INSERT));
        Option threadCountOption = new Option(BenchmarkerFlags.THREAD_COUNT_FLAG,"threads",true,"Thread count required");
        Option timeSeriesCountOption = new Option(BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,"timeSeriesCount",true,"No of time series to simulate");
        Option intervalOption = new Option(BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,"interval",true,"Average interval between observations");

        hostOption.setRequired(true);
        namespaceOption.setRequired(true);
        modeOption.setRequired(true);
        runDurationOption.setRequired(false);
        accelerationOption.setRequired(false);
        recordsPerBlockOption.setRequired(false);
        timeSeriesRangeOption.setRequired(false);
        threadCountOption.setRequired(false);
        timeSeriesCountOption.setRequired(true);
        intervalOption.setRequired(true);

        cmdLineOptions.addOption(hostOption);
        cmdLineOptions.addOption(namespaceOption);
        cmdLineOptions.addOption(modeOption);
        cmdLineOptions.addOption(runDurationOption);
        cmdLineOptions.addOption(accelerationOption);
        cmdLineOptions.addOption(recordsPerBlockOption);
        cmdLineOptions.addOption(timeSeriesRangeOption);
        cmdLineOptions.addOption(threadCountOption);
        cmdLineOptions.addOption(timeSeriesCountOption);
        cmdLineOptions.addOption(intervalOption);

        return cmdLineOptions;
    }

    /**
     * Command line options for real time insert mode
     * Allows us to check flags when this mode is used
     * @return
     */
    static Options cmdLineOptionsForRealTimeInsert(){
        Options options = cmdLineOptions();
        options.getOption(BenchmarkerFlags.TIME_SERIES_RANGE_FLAG);
        options.getOption(BenchmarkerFlags.RUN_DURATION_FLAG).setRequired(true);
        return options;
    }

    /**
     * Command line options for batch insert mode
     * Allows us to check flags when this mode is used
     * @return
     */

    static Options cmdLineOptionsforBatchInsert(){
        Options options = cmdLineOptions();
        options.getOption(BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).setRequired(true);
        return options;
    }

    /**
     * Get default value for command line flags
     * @param flag
     * @return default value for flag
     */
    /**
     * Get default value for command line flags
     * @param flag
     * @return default value for flag
     */
    private static String getDefaultValue(String flag) {
        switch (flag) {
            case BenchmarkerFlags.ACCELERATION_FLAG:
                return Integer.toString(TimeSeriesBenchmarker.DEFAULT_ACCELERATION_FACTOR);
            case BenchmarkerFlags.THREAD_COUNT_FLAG:
                return Integer.toString(TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT);
            case BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG:
                return Integer.toString(TimeSeriesBenchmarker.DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS);
            case BenchmarkerFlags.RECORDS_PER_BLOCK_FLAG:
                return Integer.toString(Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK);
            case BenchmarkerFlags.TIME_SERIES_RANGE_FLAG:
                return Integer.toString(0);
            case BenchmarkerFlags.RUN_DURATION_FLAG:
                return Integer.toString(0);
            default:
                return null;
        }
    }


    /**
     * Check type of supplied command line values
     * Throw an exception if there is a problem
     * @param flag
     * @param value
     * @throws Utilities.ParseException
     */
    private static void checkCommandLineArgumentType(String flag,String value) throws Utilities.ParseException{
        switch(flag){
            case BenchmarkerFlags.RUN_DURATION_FLAG:
            case BenchmarkerFlags.ACCELERATION_FLAG:
            case BenchmarkerFlags.THREAD_COUNT_FLAG:
            case BenchmarkerFlags.TIME_SERIES_COUNT_FLAG:
            case BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG:
                try{
                    Integer.parseInt(value);
                }
                catch(NumberFormatException e){
                    throw new Utilities.ParseException(String.format("-%s flag should have an integer argument. Argument supplied is %s",flag,value));
                }
                break;
            case BenchmarkerFlags.MODE_FLAG:
                switch(value){
                    case BenchmarkModes.BATCH_INSERT:
                    case BenchmarkModes.REAL_TIME_INSERT:
                    case BenchmarkModes.QUERY:
                        break;
                    default:
                        throw new Utilities.ParseException(String.format("-%s flag should take one of %s,%s,%s values. Argument supplied is %s",
                                flag,BenchmarkModes.BATCH_INSERT,BenchmarkModes.REAL_TIME_INSERT,BenchmarkModes.QUERY,value));
                }
        }
    }

    /**
     * Get option for optionFlag from a command line object, returning the default value if applicable
     * @param cmd
     * @param optionFlag
     * @return value for option flag
     */
    static String getOptionUsingDefaults(CommandLine cmd, String optionFlag) throws Utilities.ParseException{
        String value = cmd.getOptionValue(optionFlag, getDefaultValue(optionFlag));
        checkCommandLineArgumentType(optionFlag,value);
        return value;
    }

    /**
     * Return a CommandLine object given String[] args
     * @param args
     * @return CommandLine object
     * @throws ParseException
     */
    static CommandLine getArguments(String[] args) throws ParseException, Utilities.ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine result = parser.parse(cmdLineOptions(), args);
        switch(result.getOptionValue(BenchmarkerFlags.MODE_FLAG)){
            case BenchmarkModes.REAL_TIME_INSERT:
                if(result.hasOption(BenchmarkerFlags.TIME_SERIES_RANGE_FLAG))
                    throw new Utilities.ParseException(String.format("-%s flag should not be used in %s mode",BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,BenchmarkModes.REAL_TIME_INSERT));
        }
        return result;
    }

}
