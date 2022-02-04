package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;

import java.io.PrintStream;
import java.util.Random;

import org.apache.commons.cli.*;

public class TimeSeriesBenchmarker {

    // Simulation Defaults
    public static final int DEFAULT_TIME_SERIES_NAME_LENGTH = 10;
    public static final int DEFAULT_TIME_SERIES_COUNT = 100;
    public static final int DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS = 1;
    public static final int DEFAULT_ACCELERATION_FACTOR = 1;
    public static final int DEFAULT_RUN_DURATION_SECONDS = 10;
    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final int DEFAULT_DAILY_VOLATILITY_PCT = 10;
    public static final int DEFAULT_DAILY_DRIFT_PCT = 10;

    // In the simulation we introduce variability into the time of observations
    // The actual interval is +/- observationIntervalVariabilityPct and the simulation distributes actual time intervals uniformly across this range
    public static final int OBSERVATION_INTERVAL_VARIABILITY_PCT = 5;

    // Variables controlling the simulation
    // Those with package level visibility need to be visible to the benchmark threads
    final int averageObservationIntervalSeconds;
    final int runDuration;
    final int accelerationFactor;
    final int timeSeriesNameLength = DEFAULT_TIME_SERIES_NAME_LENGTH;
    private final int threadCount;
    private final int timeSeriesCount;
    final int recordsPerBlock;
    final int timeSeriesRangeSeconds;
    final int dailyDriftPct;
    final int dailyVolatilityPct;
    private final String runMode;
    // Seed for initialising sources of randomness
    // If a seed is supplied in the constructor this will be used else a random seed is selected
    private final long randomSeed;

    // Specify Aerospike cluster and namespace
    private final String asHost;
    private final String asNamespace;

    // Aerospike client
    private AerospikeClient aerospikeClient;
    // Underlying runnable objects for the benchmark
    private TimeSeriesRunnable[] benchmarkClientObjects;
    // Output Stream
    // Give it package protection so it can be modified by unit tests
    PrintStream output= System.out;

    // Status thread related
    private static int STATUS_UPDATE_PERIOD_SECS = 1;
    // How frequently do we check to see if a status update is needed
    private static int STATUS_TIMER_CHECK_PERIOD_MS = 50;
    // How much throughput under-performance is tolerated w/out warning
    private static int THROUGHPUT_VARIANCE_TOLERANCE_PCT = 10;


    TimeSeriesBenchmarker(String asHost, String asNamespace, String runMode, int observationIntervalSeconds, int runDurationSeconds, int accelerationFactor, int threadCount,
                          int timeSeriesCount,int recordsPerBlock, int timeSeriesRangeSeconds,int dailyDriftPct, int dailyVolatilityPct, long randomSeed){
        this.asHost = asHost;
        this.asNamespace = asNamespace;
        this.runMode = runMode;
        this.averageObservationIntervalSeconds = observationIntervalSeconds;
        this.runDuration = runDurationSeconds;
        this.accelerationFactor = accelerationFactor;
        this.threadCount = threadCount;
        this.timeSeriesCount = timeSeriesCount;
        this.recordsPerBlock = recordsPerBlock;
        this.timeSeriesRangeSeconds = timeSeriesRangeSeconds;
        this.dailyDriftPct = dailyDriftPct;
        this.dailyVolatilityPct = dailyVolatilityPct;
        this.randomSeed = randomSeed;
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
    TimeSeriesBenchmarker(String asHost, String asNamespace, int observationIntervalSeconds, int runDurationSeconds, int accelerationFactor, int threadCount,
                          int timeSeriesCount){
        this(asHost,asNamespace,OptionsHelper.BenchmarkModes.REAL_TIME_INSERT,observationIntervalSeconds,
                runDurationSeconds,accelerationFactor,threadCount,timeSeriesCount,Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK,0,
                DEFAULT_DAILY_DRIFT_PCT,DEFAULT_DAILY_VOLATILITY_PCT,new Random().nextLong());
    }

    public static void main(String[] args){
        try {
            TimeSeriesBenchmarker benchmarker = initBenchmarkerFromStringArgs(args);
            benchmarker.run();
        }
        catch(Utilities.ParseException e){
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TimeSeriesBenchmarker",OptionsHelper.cmdLineOptions());
        }
    }

    /**
     *     Helper method allowing a TimeSeriesBenchmarker to be initialised from an array of Strings - as per main method
     *     Protected visibility to allow testing use
     */
    static TimeSeriesBenchmarker initBenchmarkerFromStringArgs(String[] args) throws Utilities.ParseException{
        TimeSeriesBenchmarker benchmarker = null;
        try {
            CommandLine cmd = OptionsHelper.getArguments(args);
            benchmarker = new TimeSeriesBenchmarker(
                    OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.HOST_FLAG),
                    OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.NAMESPACE_FLAG),
                    OptionsHelper.getOptionUsingDefaults(cmd,OptionsHelper.BenchmarkerFlags.MODE_FLAG),
                    Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.INTERVAL_BETWEEN_OBSERVATIONS_SECONDS_FLAG)),
                    Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.RUN_DURATION_FLAG)),
                    Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.ACCELERATION_FLAG)),
                    Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.THREAD_COUNT_FLAG)),
                    Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, OptionsHelper.BenchmarkerFlags.TIME_SERIES_COUNT_FLAG)),
                    Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd,OptionsHelper.BenchmarkerFlags.RECORDS_PER_BLOCK_FLAG)),
                    Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd,OptionsHelper.BenchmarkerFlags.TIME_SERIES_RANGE_FLAG)),
                    DEFAULT_DAILY_DRIFT_PCT,
                    DEFAULT_DAILY_VOLATILITY_PCT,
                    new Random().nextLong()
            );
        }
        catch(ParseException e){
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TimeSeriesBenchmarker",OptionsHelper.cmdLineOptions());
            System.exit(1);
        }
        return benchmarker;
    }

    void run(){
        output.println(String.format("Updates per second : %.3f",expectedUpdatesPerSecond()));
        output.println(String.format("Updates per second per time series : %.3f",updatesPerTimeSeriesPerSecond()));
        if(updatesPerTimeSeriesPerSecond() > Constants.SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC){
            output.println(String.format("!!! Single key updates per second rate %.3f exceeds max recommended rate %d",updatesPerTimeSeriesPerSecond(),Constants.SAFE_SINGLE_KEY_UPDATE_LIMIT_PER_SEC));
        }

        // Initialisation
        aerospikeClient = new AerospikeClient(asHost,Constants.DEFAULT_AEROSPIKE_PORT);
        aerospikeClient.truncate(new InfoPolicy(),asNamespace,Constants.AS_TIME_SERIES_SET,null);
        aerospikeClient.truncate(new InfoPolicy(),asNamespace,Constants.AS_TIME_SERIES_INDEX_SET,null);

        benchmarkClientObjects = new TimeSeriesRunnable[threadCount];
        for(int i=0;i<threadCount;i++){
            int timeSeriesCountForThread = timeSeriesCount /  threadCount;
            if(i < timeSeriesCount % threadCount) timeSeriesCountForThread++;
            TimeSeriesRunnable runnable = null; // Keep the compiler happy with null assignment
            switch(runMode){
                case OptionsHelper.BenchmarkModes.REAL_TIME_INSERT:
                    runnable = new RealTimeInsertTimeSeriesRunnable(asHost,asNamespace,timeSeriesCountForThread,this,randomSeed); break;
                case OptionsHelper.BenchmarkModes.BATCH_INSERT:
                    runnable = new BatchInsertTimeSeriesRunnable(asHost,asNamespace,timeSeriesCountForThread,this,randomSeed);
            }
            benchmarkClientObjects[i] = runnable;
            Thread t = new Thread(benchmarkClientObjects[i]);
            t.start();
        }
        long nextOutputTime = System.currentTimeMillis();
        while(isRunning()){
            if(System.currentTimeMillis() > nextOutputTime) {
                if(averageThreadRunTimeMs() >0) outputStatus();
                nextOutputTime+= Constants.MILLISECONDS_IN_SECOND * STATUS_UPDATE_PERIOD_SECS;
                try {
                    Thread.sleep(STATUS_TIMER_CHECK_PERIOD_MS);
                }
                catch(InterruptedException e){}
            }
        }
        outputStatus();
    }

    /**
     * Output current status of simulation
     * Will give a warning if it is running slower than expected
     */
    private void outputStatus(){
        output.println(String.format("Run time :  %d seconds, Update count : %d, Actual updates per second : %.3f", averageThreadRunTimeMs() / Constants.MILLISECONDS_IN_SECOND,
                totalUpdateCount(), (double) Constants.MILLISECONDS_IN_SECOND * totalUpdateCount()/ averageThreadRunTimeMs()));
        // If the no of updates per second is *less* than expected updates per second (to a given tolerance)
        // And we are beyond the first second (can produce anomalous results )
        // show a warning message to that effect
        double actualUpdateRate = (double) Constants.MILLISECONDS_IN_SECOND * totalUpdateCount() / averageThreadRunTimeMs();
        if((averageThreadRunTimeMs() >= Constants.MILLISECONDS_IN_SECOND) && (expectedUpdatesPerSecond() > actualUpdateRate)) {
            if (!Utilities.valueInTolerance(expectedUpdatesPerSecond(), actualUpdateRate, THROUGHPUT_VARIANCE_TOLERANCE_PCT)) {
                output.println(String.format("!!!Update rate should be %.3f, actually %.3f - underflow",
                        expectedUpdatesPerSecond(), actualUpdateRate));
            }
        }
    }

    /**
     * Is the simulation still running - check each of the threads
     * @return True if at least one thread is still running, else false
     */
    private boolean isRunning(){
        boolean running = false;
        for(int i=0;i<benchmarkClientObjects.length;i++){
            running |= benchmarkClientObjects[i].isRunning();
        }
        return running;
    }

    /**
     * Compute the simulation duration by looking at the average thread run time
     * Return value is in milliseconds
     * Package level visibility for testing purposes
     * @return
     */
    long averageThreadRunTimeMs(){
        long runTime = 0;
        for(int i=0;i<benchmarkClientObjects.length;i++){
            runTime += benchmarkClientObjects[i].runTime();
        }
        return runTime / benchmarkClientObjects.length;
    }

    /**
     * Compute the total number of inserts for the simulation
     * Do this by aggregating inserts per thread
     * Package level visibility for testing purposes*
     * @return
     */
    int totalUpdateCount(){
        int totalUpdateCount = 0;
        for(int i=0;i<benchmarkClientObjects.length;i++){
            totalUpdateCount += benchmarkClientObjects[i].getUpdateCount();
        }
        return totalUpdateCount;
    }


    /**
     * Work out the actual number of updates per second for the simulation - allowing for the acceleration
     * Package level access to allow for testing
     * @return
     */
    double expectedUpdatesPerSecond(){
        return (double)accelerationFactor * timeSeriesCount / averageObservationIntervalSeconds;
    }

    /**
     * Actual number of updates per time series per second - allowing for the acceleration
     * @return
     */
    private double updatesPerTimeSeriesPerSecond(){
        return (double)accelerationFactor / averageObservationIntervalSeconds;
    }

}
