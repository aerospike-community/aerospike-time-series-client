package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import io.github.ken_tune.time_series.Constants;

public class TimeSeriesBenchmarker {

    // Simulation Defaults
    public static final int DEFAULT_TIME_SERIES_NAME_LENGTH = 10;
    public static final int DEFAULT_TIME_SERIES_COUNT = 100;
    public static final int DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS = 1;
    public static final int DEFAULT_ACCELERATION_FACTOR = 10;
    public static final int DEFAULT_RUN_DURATION_SECONDS = 10;
    public static final int DEFAULT_THREAD_COUNT = 10;
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

    // Specify Aerospike cluster and namespace
    private final String asHost;
    private final String asNamespace;

    // Aerospike client
    private AerospikeClient aerospikeClient;
    // Underlying runnable objects for the benchmark
    private TimeSeriesBenchmarkRunnable[] benchmarkClientObjects;

    TimeSeriesBenchmarker(String asHost, String asNamespace, int observationIntervalSeconds, int runDurationSeconds, int accelerationFactor, int threadCount, int timeSeriesCount){
        this.asHost = asHost;
        this.asNamespace = asNamespace;
        this.averageObservationIntervalSeconds = observationIntervalSeconds;
        this.runDuration = runDurationSeconds;
        this.accelerationFactor = accelerationFactor;
        this.threadCount = threadCount;
        this.timeSeriesCount = timeSeriesCount;
    }

    public static void main(String[] args){
        String AEROSPIKE_HOST="172.28.128.7";
        String AEROSPIKE_NAMESPACE="test";

        TimeSeriesBenchmarker benchmarker =
                new TimeSeriesBenchmarker(AEROSPIKE_HOST,AEROSPIKE_NAMESPACE,DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS,
                        DEFAULT_RUN_DURATION_SECONDS,DEFAULT_ACCELERATION_FACTOR,DEFAULT_THREAD_COUNT,DEFAULT_TIME_SERIES_COUNT);
        benchmarker.run();
    }

    void run(){
        System.out.println(String.format("Updates per second : %.3f",expectedUpdatesPerSecond()));
        System.out.println(String.format("Updates per second per time series : %.3f",updatesPerTimeSeriesPerSecond()));

        // Initialisation
        aerospikeClient = new AerospikeClient(asHost,3000);
        aerospikeClient.truncate(new InfoPolicy(),asNamespace,Constants.AS_TIME_SERIES_SET,null);
        aerospikeClient.truncate(new InfoPolicy(),asNamespace,Constants.AS_TIME_SERIES_INDEX_SET,null);

        benchmarkClientObjects = new TimeSeriesBenchmarkRunnable[threadCount];
        for(int i=0;i<threadCount;i++){
            int timeSeriesCountForThread = timeSeriesCount /  threadCount;
            if(i < timeSeriesCount % threadCount) timeSeriesCountForThread++;
            benchmarkClientObjects[i] = new TimeSeriesBenchmarkRunnable(asHost,asNamespace,timeSeriesCountForThread,this);
            Thread t = new Thread(benchmarkClientObjects[i]);
            t.start();
        }
        long nextOutputTime = System.currentTimeMillis();
        while(isRunning()){
            if(System.currentTimeMillis() > nextOutputTime) {
                if(averageThreadRunTimeMs() >0) outputStatus();
                nextOutputTime+=1000;
                try {
                    Thread.sleep(50);
                }
                catch(InterruptedException e){}
            }
        }
        outputStatus();
    }

    private void outputStatus(){
        System.out.println(String.format("Run time :  %d seconds, Update count : %d, Actual updates per second : %.3f", averageThreadRunTimeMs() / 1000,
                totalUpdateCount(), (double)1000 * totalUpdateCount()/ averageThreadRunTimeMs()));
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
     * @return
     */
    private double expectedUpdatesPerSecond(){
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
