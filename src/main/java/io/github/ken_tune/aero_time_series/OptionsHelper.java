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
        static final String RUN_DURATION_FLAG = "d";
        static final String ACCELERATION_FLAG = "a";
        static final String THREAD_COUNT_FLAG = "z";
        static final String TIME_SERIES_COUNT_FLAG = "c";
        static final String INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG = "p";
    }
    
    /**
     * Command line options for Main class
     * @return cmdLineOptions
     */
    static Options cmdLineOptions(){
        Options cmdLineOptions = new Options();

        Option hostOption = new Option(BenchmarkerFlags.HOST_FLAG,"host",true,"Aerospike seed host");
        Option namespaceOption = new Option(BenchmarkerFlags.NAMESPACE_FLAG,"namespace",true,"Namespace");
        Option runDurationOption = new Option(BenchmarkerFlags.RUN_DURATION_FLAG,"duration",true,"Simulation duration in seconds");
        Option accelerationOption = new Option(BenchmarkerFlags.ACCELERATION_FLAG,"acceleration",true,"Simulation acceleration factor (clock speed multiplier)");
        Option threadCountOption = new Option(BenchmarkerFlags.THREAD_COUNT_FLAG,"threads",true,"Thread count required");
        Option timeSeriesCountOption = new Option(BenchmarkerFlags.TIME_SERIES_COUNT_FLAG,"timeSeriesCount",true,"No of time series to simulate");
        Option intervalOption = new Option(BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG,"interval",true,"Average interval between observations");

        hostOption.setRequired(true);
        namespaceOption.setRequired(true);
        runDurationOption.setRequired(true);
        accelerationOption.setRequired(false);
        threadCountOption.setRequired(false);
        timeSeriesCountOption.setRequired(true);
        intervalOption.setRequired(true);

        cmdLineOptions.addOption(hostOption);
        cmdLineOptions.addOption(namespaceOption);
        cmdLineOptions.addOption(runDurationOption);
        cmdLineOptions.addOption(accelerationOption);
        cmdLineOptions.addOption(threadCountOption);
        cmdLineOptions.addOption(timeSeriesCountOption);
        cmdLineOptions.addOption(intervalOption);

        return cmdLineOptions;
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
                    throw new Utilities.ParseException(String.format("-%s flag should have an integer argument. Argument supplied is  %s",flag,value));
                }
                break;
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
    static CommandLine getArguments(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(cmdLineOptions(), args);
    }

}
