package io.github.aerospike_examples.aero_time_series.benchmark;

import io.github.aerospike_examples.aero_time_series.Constants;
import io.github.aerospike_examples.aero_time_series.Utilities;
import org.apache.commons.cli.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Help with command line processing
 */
public class OptionsHelper {
    /**
     * Flags used at the command line
     */
    public static class BenchmarkerFlags{
        public static final String HOST_FLAG = "h";
        public static final String NAMESPACE_FLAG = "n";
        public static final String MODE_FLAG = "m";
        public static final String RUN_DURATION_FLAG = "d";
        public static final String ACCELERATION_FLAG = "a";
        public static final String RECORDS_PER_BLOCK_FLAG = "b";
        public static final String THREAD_COUNT_FLAG = "z";
        public static final String TIME_SERIES_COUNT_FLAG = "c";
        public static final String INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG = "p";
        public static final String TIME_SERIES_RANGE_FLAG = "r";
        public static final String TIME_SERIES_SET_FLAG = "s";
        public static final String TIME_SERIES_NAME_FLAG = "i";
    }

    public static class BenchmarkModes{
        public static final String REAL_TIME_INSERT = "realTimeWrite";
        public static final String BATCH_INSERT = "batchInsert";
        public static final String QUERY = "query";
    }

    static class TimeUnitIndicators{
        static final String YEAR="Y";
        static final String DAY="D";
        static final String HOUR="H";
        static final String MINUTE="M";
        static final String SECOND="S";
        // Utility string for messages
        static final String ALL_INDICATORS=String.format("%s,%s,%s,%s or %s",YEAR,DAY,HOUR,MINUTE,SECOND);
    }

    // Regex time strings need to match to
    // Package visibility so test classes can use
    private static final Pattern regexForTimeStrings = Pattern.compile("^\\d+[YDHMS]*$");

    /**
     * Command line options for Main class
     * This function provides the options in general terms, to allow for initial parsing
     * Separate logic is required to parse the options based on benchmark mode - this is done in getArguments
     * Utility functions are provided below to return command line options for each mode
     * @return standardCmdLineOptions
     */
    static Options standardCmdLineOptions(){
        Options cmdLineOptions = new Options();

        Option hostOption = new Option(BenchmarkerFlags.HOST_FLAG,"host",true,"Aerospike seed host. Required");
        Option namespaceOption = new Option(BenchmarkerFlags.NAMESPACE_FLAG,"namespace",true,"Namespace for time series. Required.");
        Option setOption = new Option(BenchmarkerFlags.TIME_SERIES_SET_FLAG,"set",true,
                String.format("Set for time series. Defaults to %s",Constants.DEFAULT_TIME_SERIES_SET));
        Option modeOption = new Option(BenchmarkerFlags.MODE_FLAG,"mode",true,
                String.format("Benchmark mode - values allowed are %s, %s and %s. Required.",
                        BenchmarkModes.REAL_TIME_INSERT,BenchmarkModes.BATCH_INSERT, BenchmarkModes.QUERY));
        Option runDurationOption = new Option(BenchmarkerFlags.RUN_DURATION_FLAG,"duration",true,
                String.format("Simulation duration in seconds. Required for %s and %s mode. Not valid in %s mode",
                        BenchmarkModes.REAL_TIME_INSERT, BenchmarkModes.QUERY, BenchmarkModes.BATCH_INSERT));
        Option accelerationOption = new Option(BenchmarkerFlags.ACCELERATION_FLAG,"acceleration",true,
                String.format("Simulation acceleration factor (clock speed multiplier). Only valid in %s mode. Optional.",BenchmarkModes.REAL_TIME_INSERT));
        Option recordsPerBlockOption = new Option(BenchmarkerFlags.RECORDS_PER_BLOCK_FLAG,"recordsPerBlock",true,
                String.format("Max time series points in each Aerospike object. Optional. Defaults to %d",Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK));
        //noinspection SpellCheckingInspection
        Option timeSeriesRangeOption = new Option(BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,"timeSeriesRange",true,
                String.format("Period to be spanned by time series. Only valid in %s mode.\n" +
                        "Specify as <number><unit> where <unit is one of Y(ears),D(ays),H(ours),M(inutes),S(econds) e.g. 1Y or 12H",
                        BenchmarkModes.BATCH_INSERT));
        Option threadCountOption = new Option(BenchmarkerFlags.THREAD_COUNT_FLAG,"threads",true,
                String.format("Thread count required. Optional. Defaults to %d",TimeSeriesBenchmarker.DEFAULT_THREAD_COUNT));
        Option timeSeriesCountOption = new Option(BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,"timeSeriesCount",true,
                String.format("No of time series to simulate. Optional. Defaults to %d",TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT));
        Option intervalOption = new Option(BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,"interval",true,
                "Average interval between observations. Required");

        // These options are common to all modes
        hostOption.setRequired(true);
        namespaceOption.setRequired(true);
        setOption.setRequired(false);
        modeOption.setRequired(true);
        threadCountOption.setRequired(false);

        // The options below - whether these options are allowed / optional / not allowed is mode dependent
        runDurationOption.setRequired(false);
        accelerationOption.setRequired(false);
        recordsPerBlockOption.setRequired(false);
        timeSeriesRangeOption.setRequired(false);
        timeSeriesCountOption.setRequired(false);
        intervalOption.setRequired(false);

        cmdLineOptions.addOption(hostOption);
        cmdLineOptions.addOption(namespaceOption);
        cmdLineOptions.addOption(setOption);
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
     * @return Options object
     */
    private static Options cmdLineOptionsForRealTimeInsert(){
        Options options = standardCmdLineOptions();
        Options clonedOptions = new Options();
        // Basically need to filter out the options which are not allowed in this mode
        for(Option option : options.getOptions()){
            switch(option.getOpt()){
                case BenchmarkerFlags.TIME_SERIES_RANGE_FLAG:
                    break;
                default:
                    clonedOptions.addOption(option);
            }
        }
        // And set required for the flags that are needed
        options.getOption(BenchmarkerFlags.RUN_DURATION_FLAG).setRequired(true);
        return clonedOptions;
    }

    /**
     * Command line options for batch insert mode
     * Allows us to check flags when this mode is used
     * @return Options object
     */
    private static Options cmdLineOptionsForBatchInsert(){
        Options options = standardCmdLineOptions();
        Options clonedOptions = new Options();
        // Basically need to filter out the options which are not allowed in this mode
        for(Option option : options.getOptions()){
            switch(option.getOpt()){
                case BenchmarkerFlags.RUN_DURATION_FLAG:
                case BenchmarkerFlags.ACCELERATION_FLAG:
                    break;
                default:
                    clonedOptions.addOption(option);
            }
        }
        // And set required for the flags that are needed
        options.getOption(BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).setRequired(true);
        return clonedOptions;
    }

    /**
     * Command line options for query mode
     * Allows us to check flags when this mode is used
     * @return Options object
     */
    private static Options cmdLineOptionsForQueryMode(){
        Options options = standardCmdLineOptions();
        Options clonedOptions = new Options();
        // Basically need to filter out the options which are not allowed in this mode
        for(Option option : options.getOptions()){
            switch(option.getOpt()){
                case BenchmarkerFlags.ACCELERATION_FLAG:
                case BenchmarkerFlags.TIME_SERIES_RANGE_FLAG:
                case BenchmarkerFlags.TIME_SERIES_COUNT_FLAG:
                case BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG:
                    break;
                default:
                    clonedOptions.addOption(option);
            }
        }
        // And set required for the flags that are needed
        clonedOptions.getOption(BenchmarkerFlags.RUN_DURATION_FLAG).setRequired(true);
        return clonedOptions;
    }

    /**
     * Command line options for Main class
     * This function provides the options in general terms, to allow for initial parsing
     * Separate logic is required to parse the options based on benchmark mode - this is done in getArguments
     * Utility functions are provided below to return command line options for each mode
     * @return standardCmdLineOptions
     */
    static Options cmdLineOptionsForReader() {
        Options cmdLineOptions = new Options();

        Option hostOption = new Option(io.github.aerospike_examples.aero_time_series.benchmark.OptionsHelper.BenchmarkerFlags.HOST_FLAG, "host", true, "Aerospike seed host. Required");
        Option namespaceOption = new Option(io.github.aerospike_examples.aero_time_series.benchmark.OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG, "namespace", true, "Namespace for time series. Required.");
        Option setOption = new Option(io.github.aerospike_examples.aero_time_series.benchmark.OptionsHelper.BenchmarkerFlags.TIME_SERIES_SET_FLAG, "set", true,
                String.format("Set for time series. Defaults to %s", Constants.DEFAULT_TIME_SERIES_SET));
        Option timeSeriesNameOption  = new Option(io.github.aerospike_examples.aero_time_series.benchmark.OptionsHelper.BenchmarkerFlags.TIME_SERIES_NAME_FLAG,
                "timeSeriesName",true,"Name of time series");

        // These options are common to all modes
        hostOption.setRequired(true);
        namespaceOption.setRequired(true);
        setOption.setRequired(false);
        timeSeriesNameOption.setRequired(false);

        cmdLineOptions.addOption(hostOption);
        cmdLineOptions.addOption(namespaceOption);
        cmdLineOptions.addOption(setOption);
        cmdLineOptions.addOption(timeSeriesNameOption);
        return cmdLineOptions;
    }

    /**
     * Get default value for command line flags
     * @param flag flag identifier
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
            case BenchmarkerFlags.TIME_SERIES_SET_FLAG:
                return Constants.DEFAULT_TIME_SERIES_SET;
            case BenchmarkerFlags.TIME_SERIES_COUNT_FLAG:
                return Integer.toString(TimeSeriesBenchmarker.DEFAULT_TIME_SERIES_COUNT);
            default:
                return null;
        }
    }


    /**
     * Check type of supplied command line values
     * Throw an exception if there is a problem
     * @param flag flag indicating which parameter option we're checking
     * @param value value we're checking
     * @throws Utilities.ParseException if value cannot be parsed as expected
     */
    private static void checkCommandLineArgumentType(String flag,String value) throws Utilities.ParseException{
        switch(flag){
            // Absence of breaks is deliberate - all the below are int fields
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
                break;
            case BenchmarkerFlags.TIME_SERIES_RANGE_FLAG:
                checkTimeString(value);
        }
    }

    /**
     * Get option for optionFlag from a command line object, returning the default value if applicable
     * @param cmd CommandLine object has command line arguments as called
     * @param optionFlag option value is required for
     * @return value for option flag
     */
    static String getOptionUsingDefaults(CommandLine cmd, String optionFlag) throws Utilities.ParseException{
        String value = cmd.getOptionValue(optionFlag, getDefaultValue(optionFlag));
        if(cmd.hasOption(optionFlag)) checkCommandLineArgumentType(optionFlag,value);
        return value;
    }

    /**
     * Return a CommandLine object given String[] args
     * @param args String[] as would be passed at cmd line
     * @return CommandLine object
     * @throws ParseException if options cannot be parsed
     */
    static CommandLine getArguments(String[] args) throws ParseException, Utilities.ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine result = parser.parse(standardCmdLineOptions(), args);
        switch(result.getOptionValue(BenchmarkerFlags.MODE_FLAG)){
            case BenchmarkModes.REAL_TIME_INSERT:
                for(Option option : OptionsHelper.standardCmdLineOptions().getOptions()) {
                    if (!OptionsHelper.cmdLineOptionsForRealTimeInsert().hasOption(option.getOpt())){
                        throwErrorIfFlagFoundForMode(result,option.getOpt(),result.getOptionValue(BenchmarkerFlags.MODE_FLAG));
                    }
                }
                break;
            case BenchmarkModes.BATCH_INSERT:
                for(Option option : OptionsHelper.standardCmdLineOptions().getOptions()) {
                    if (!OptionsHelper.cmdLineOptionsForBatchInsert().hasOption(option.getOpt())){
                        throwErrorIfFlagFoundForMode(result,option.getOpt(),result.getOptionValue(BenchmarkerFlags.MODE_FLAG));
                    }
                }
                break;
            case BenchmarkModes.QUERY:
                for(Option option : OptionsHelper.standardCmdLineOptions().getOptions()) {
                    if (!OptionsHelper.cmdLineOptionsForQueryMode().hasOption(option.getOpt())){
                        throwErrorIfFlagFoundForMode(result,option.getOpt(),result.getOptionValue(BenchmarkerFlags.MODE_FLAG));
                    }
                }
                break;
            default:
                throw new Utilities.ParseException(
                    String.format("%s is an invalid run mode. Please use %s, %s or %s",result.getOptionValue(BenchmarkerFlags.MODE_FLAG),
                            BenchmarkModes.REAL_TIME_INSERT,BenchmarkModes.BATCH_INSERT,BenchmarkModes.QUERY));
        }
        return result;
    }

    /**
     * Small utility function to throw an error if specified flag is found for given mode
     * @param result CommandLine object
     * @param benchmarkerFlag flag to check for
     * @param benchmarkerMode run mode
     * @throws Utilities.ParseException if flag is being used
     */
    private static void throwErrorIfFlagFoundForMode(CommandLine result, String benchmarkerFlag,String benchmarkerMode) throws Utilities.ParseException{
        if(result.hasOption(benchmarkerFlag))
            throw new Utilities.ParseException(String.format("-%s flag (%s) should not be used in %s mode",
                    benchmarkerFlag,
                    OptionsHelper.standardCmdLineOptions().getOption(benchmarkerFlag).getLongOpt(),
                    benchmarkerMode));
    }
    /**
     * Convert timeString to seconds based on the unit provided
     * Assume seconds if no unit provided
     * Throws an error if the timeString is not in the expected format i.e. an integer or integer followed by one of S,M,H,D,Y
     *
     * @param timeString time represented as a string
     * @return time in seconds
     * @throws Utilities.ParseException if timeString cannot be parsed
     */
    static int convertTimeStringToSeconds(String timeString) throws Utilities.ParseException{
        int timePart;
        int multiplier = 1;
        checkTimeString(timeString);
        switch(timeString.substring(timeString.length() -1)){
            case TimeUnitIndicators.SECOND:
                multiplier=1;
                timePart = Integer.parseInt(timeString.substring(0,timeString.length() -1));
                break;
            case TimeUnitIndicators.MINUTE:
                multiplier = 60;
                timePart = Integer.parseInt(timeString.substring(0,timeString.length() -1));
                break;
            case TimeUnitIndicators.HOUR:
                multiplier = 60*60;
                timePart = Integer.parseInt(timeString.substring(0,timeString.length() -1));
                break;
            case TimeUnitIndicators.DAY:
                multiplier = 24 * 60 * 60;
                timePart = Integer.parseInt(timeString.substring(0,timeString.length() -1));
                break;
            case TimeUnitIndicators.YEAR:
                multiplier = 24 * 60 * 60 * 365;
                timePart = Integer.parseInt(timeString.substring(0,timeString.length() -1));
                break;
            default:
                timePart = Integer.parseInt(timeString);
        }
        return multiplier * timePart;
    }

    /**
     * Utility method to check the format of time strings, which should be
     * number followed by one of Y,D,H,M,S or no suffix
     * If the format is not followed, an exception will be thrown
     *
     * @param timeString string to check
     */
    private static void checkTimeString(String timeString) throws Utilities.ParseException{
        Matcher matcher = regexForTimeStrings.matcher(timeString);
        if(!matcher.find()) {
            String errorMessage =
                    String.format("Value for %s flag should be one of <integer> followed by %s, indicating years, days, hours, minutes or seconds\n",
                            BenchmarkerFlags.TIME_SERIES_RANGE_FLAG, TimeUnitIndicators.ALL_INDICATORS);
            errorMessage += "If no unit, number is interpreted as seconds\n";
            errorMessage += String.format("Supplied value %s does not match this format", timeString);
            throw new Utilities.ParseException(errorMessage);
        }

    }
}
