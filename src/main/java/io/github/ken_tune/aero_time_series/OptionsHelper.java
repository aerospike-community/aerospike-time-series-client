package io.github.ken_tune.aero_time_series;

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
    static Pattern regexForTimeStrings = Pattern.compile("^\\d+(Y|D|H|M|S)*$");

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
        options.getOption(BenchmarkerFlags.RUN_DURATION_FLAG).setRequired(true);
        options.getOption(BenchmarkerFlags.ACCELERATION_FLAG).setRequired(false);
        return options;
    }

    /**
     * Command line options for batch insert mode
     * Allows us to check flags when this mode is used
     * @return
     */

    static Options cmdLineOptionsForBatchInsert(){
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
                    throw new Utilities.ParseException(String.format("-%s flag (%s) should not be used in %s mode",
                            BenchmarkerFlags.TIME_SERIES_RANGE_FLAG,
                            OptionsHelper.cmdLineOptionsForRealTimeInsert().getOption(BenchmarkerFlags.TIME_SERIES_RANGE_FLAG).getLongOpt(),
                            BenchmarkModes.REAL_TIME_INSERT));
                break;
            case BenchmarkModes.BATCH_INSERT:
                if(result.hasOption(BenchmarkerFlags.RUN_DURATION_FLAG))
                    throw new Utilities.ParseException(String.format("-%s flag (%s) should not be used in %s mode",
                            BenchmarkerFlags.RUN_DURATION_FLAG,
                            OptionsHelper.cmdLineOptionsForBatchInsert().getOption(BenchmarkerFlags.RUN_DURATION_FLAG).getLongOpt(),
                            BenchmarkModes.BATCH_INSERT));
                if(result.hasOption(BenchmarkerFlags.ACCELERATION_FLAG))
                    throw new Utilities.ParseException(String.format("-%s flag (%s) should not be used in %s mode",
                            BenchmarkerFlags.ACCELERATION_FLAG,
                            OptionsHelper.cmdLineOptionsForBatchInsert().getOption(BenchmarkerFlags.ACCELERATION_FLAG).getLongOpt(),
                            BenchmarkModes.BATCH_INSERT));
                break;
            default:
                throw new Utilities.ParseException(
                    String.format("%s is an invalid run mode. Please use %s or %s",result.getOptionValue(BenchmarkerFlags.MODE_FLAG),
                            BenchmarkModes.REAL_TIME_INSERT,BenchmarkModes.BATCH_INSERT));
        }
        return result;
    }

    /**
     * Convert timeString to seconds based on the unit provided
     * Assume seconds if no unit provided
     * Throws an error if the timeString is not in the expected format i.e. an integer or integer followed by one of S,M,H,D,Y
     *
     * @param timeString
     * @return
     * @throws Utilities.ParseException
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
     * @param timeString
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
