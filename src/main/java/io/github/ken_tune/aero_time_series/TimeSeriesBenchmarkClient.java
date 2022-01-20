package io.github.ken_tune.aero_time_series;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import io.github.ken_tune.time_series.DataPoint;
import io.github.ken_tune.time_series.TimeSeriesClient;
import io.github.ken_tune.time_series.Constants;

import java.util.*;

public class TimeSeriesBenchmarkClient implements Runnable{
    // Seed for random number generation
    private static long RANDOM_SEED = 6760187239798559903L;
    private Random randomNumberGenerator = new Random(RANDOM_SEED);

    // Defaults
    public static final int DEFAULT_TIME_SERIES_NAME_LENGTH = 10;
    public static final int DEFAULT_TIME_SERIES_COUNT = 100;
    public static final int DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS = 10;
    public static final int DEFAULT_ACCELERATION_FACTOR = 10;
    public static final int DEFAULT_RUN_DURATION_SECONDS = 10;
    public static final int DEFAULT_THREAD_COUNT = 10;

    // Member variables
    private String aerospikeHost;
    private String aerospikeNamespace;
    private TimeSeriesClient timeSeriesClient;
    private int timeSeriesCountPerObject;
    private long simulationStartTime;
    private int updateCount = 0;
    private boolean running = true;

    private static final int averageObservationIntervalSeconds = DEFAULT_AVERAGE_OBSERVATION_INTERVAL_SECONDS;
    private static final int runDuration = DEFAULT_RUN_DURATION_SECONDS;
    private static final int accelerationFactor = DEFAULT_ACCELERATION_FACTOR;
    private static final int timeSeriesNameLength = DEFAULT_TIME_SERIES_NAME_LENGTH;
    private static final int threadCount = DEFAULT_THREAD_COUNT;
    private static final int timeSeriesCount = DEFAULT_TIME_SERIES_COUNT;


    private static AerospikeClient aerospikeClient;
    private static TimeSeriesBenchmarkClient[] benchmarkClientObjects;

    public static void main(String[] args){
        String AEROSPIKE_HOST="172.28.128.7";
        String AEROSPIKE_NAMESPACE="test";

        // Initialisation
        aerospikeClient = new AerospikeClient(AEROSPIKE_HOST,3000);
        aerospikeClient.truncate(new InfoPolicy(),AEROSPIKE_NAMESPACE,Constants.AS_TIME_SERIES_SET,null);
        aerospikeClient.truncate(new InfoPolicy(),AEROSPIKE_NAMESPACE,Constants.AS_TIME_SERIES_INDEX_SET,null);
        System.out.println(String.format("Updates per second : %.3f",expectedUpdatesPerSecond()));
        System.out.println(String.format("Updates per second per time series : %.3f",updatesPerTimeSeriesPerSecond()));

        benchmarkClientObjects = new TimeSeriesBenchmarkClient[threadCount];
        for(int i=0;i<threadCount;i++){
            int timeSeriesCountForThread = timeSeriesCount /  threadCount;
            if(i < timeSeriesCount % threadCount) timeSeriesCountForThread++;
            benchmarkClientObjects[i] = new TimeSeriesBenchmarkClient(AEROSPIKE_HOST,AEROSPIKE_NAMESPACE,timeSeriesCountForThread);
            Thread t = new Thread(benchmarkClientObjects[i]);
            t.start();
        }
        long nextOutputTime = System.currentTimeMillis();
        while(isRunning()){
            if(System.currentTimeMillis() > nextOutputTime) {
                if(averageThreadRunTime() >0) outputStatus();
                nextOutputTime+=1000;
                try {
                    Thread.sleep(50);
                }
                catch(InterruptedException e){}
            }
        }
        outputStatus();
    }

    private static void outputStatus(){
        System.out.println(String.format("Run time :  %d seconds, Update count : %d, Actual updates per second : %.3f", averageThreadRunTime(),
                totalUpdateCount(), (double)totalUpdateCount()/averageThreadRunTime()));
    }

    private static boolean isRunning(){
        boolean running = false;
        for(int i=0;i<benchmarkClientObjects.length;i++){
            running |= benchmarkClientObjects[i].running;
        }
        return running;
    }

    private static long averageThreadRunTime(){
        long runTime = 0;
        for(int i=0;i<benchmarkClientObjects.length;i++){
            runTime += benchmarkClientObjects[i].actualRunTime();
        }
        return runTime / benchmarkClientObjects.length;
    }

    private static int totalUpdateCount(){
        int totalUpdateCount = 0;
        for(int i=0;i<benchmarkClientObjects.length;i++){
            totalUpdateCount += benchmarkClientObjects[i].updateCount();
        }
        return totalUpdateCount;

    }

    public TimeSeriesBenchmarkClient(String host,String namespace,int timeSeriesCountPerObject){
        aerospikeHost = host;
        aerospikeNamespace = namespace;
        this.timeSeriesCountPerObject = timeSeriesCountPerObject;
    }

    private static double expectedUpdatesPerSecond(){
        return (double)accelerationFactor * timeSeriesCount / averageObservationIntervalSeconds;
    }

    private static double updatesPerTimeSeriesPerSecond(){
        return (double)accelerationFactor / averageObservationIntervalSeconds;
    }

    private int updateCount(){
        return updateCount;
    }

    private long actualRunTime(){
        return (System.currentTimeMillis() - simulationStartTime)/1000;
    }

    public void run(){
        simulationStartTime = System.currentTimeMillis();
        timeSeriesClient = new TimeSeriesClient(aerospikeHost,aerospikeNamespace);

        Map<String,Long> nextObservationTimes = new HashMap<>();
        for(int i = 0; i< timeSeriesCountPerObject; i++){
            nextObservationTimes.put(randomTimeSeriesName(),0L);
        }
        running = true;
        while(getSimulationTime() - simulationStartTime < runDuration * 1000 * accelerationFactor){
            Iterator<String> timeSeriesNames = nextObservationTimes.keySet().iterator();
            while(timeSeriesNames.hasNext()){
                String timeSeriesName = timeSeriesNames.next();
                long nextObservationTime = nextObservationTimes.get(timeSeriesName);
                if(nextObservationTime < getSimulationTime()) {
                    updateCount++;
                    timeSeriesClient.put(timeSeriesName,new DataPoint(new Date(nextObservationTime),randomNumberGenerator.nextDouble()));
                    nextObservationTimes.put(timeSeriesName,nextObservationTime());
                }
            }
        }
        running= false;
    }

    private long getSimulationTime(){
        return simulationStartTime + (System.currentTimeMillis() - simulationStartTime) * accelerationFactor;
    }
    private long nextObservationTime(){
        return getSimulationTime() + (int)(averageObservationIntervalSeconds*(950+(0.1*randomNumberGenerator.nextInt(1000))));
    }

    public int getTimeSeriesNameLength() {
        return timeSeriesNameLength;
    }

    /**
     * Randomly generate a time series name (a String) of length timeSeriesNameLength
     * Package level visibility for testing purposes
     *
     * @return String:timeSeriesName
     */
    String randomTimeSeriesName(){
        char[] availableCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        char[] timeSeriesName = new char[timeSeriesNameLength];
        for(int i=0;i<timeSeriesNameLength;i++) timeSeriesName[i] = availableCharacters[randomNumberGenerator.nextInt(availableCharacters.length)];
        return String.valueOf(timeSeriesName);
    }

}
