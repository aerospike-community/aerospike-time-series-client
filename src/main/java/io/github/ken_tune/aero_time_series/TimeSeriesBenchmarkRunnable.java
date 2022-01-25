package io.github.ken_tune.aero_time_series;

import io.github.ken_tune.time_series.DataPoint;
import io.github.ken_tune.time_series.TimeSeriesClient;

import java.util.*;

// Implementation of individual runnable TimeSeriesBenchmark object
// Not intended for direct use - hence package level visibility
class TimeSeriesBenchmarkRunnable implements Runnable {
    // Seed for randomiser
    private static final long RANDOM_SEED = 6760187239798559903L;
    private static final Random randomNumberGenerator = new Random(RANDOM_SEED);

    // Member variables
    private TimeSeriesClient timeSeriesClient;
    private int timeSeriesCountPerObject;
    private long startTime;
    private int runDurationSeconds;
    private int accelerationFactor;
    private int updateCount = 0;
    // isRunning indicates the simulation is active
    // Set to true at initialisation time. Set to false when finished
    private boolean isRunning = true;
    // Has the simulation finished (allows us to distinguish between start and end)
    private boolean isFinished = false;
    private int timeSeriesNameLength;
    // Average time interval in seconds between successive each time series observation
    private int observationIntervalSeconds;
    // In the simulation we introduce variability into the time of observations
    // The actual interval is +/- observationIntervalVariabilityPct and the simulation distributes actual time intervals uniformly across this range
    private double observationIntervalVariabilityPct = 5;

    public TimeSeriesBenchmarkRunnable(String asHost, String asNamespace, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient){
        timeSeriesClient = new TimeSeriesClient(asHost,asNamespace);
        this.timeSeriesCountPerObject = timeSeriesCountPerObject;
        this.runDurationSeconds = benchmarkClient.runDuration;
        this.accelerationFactor = benchmarkClient.accelerationFactor;
        this.timeSeriesNameLength = benchmarkClient.timeSeriesNameLength;
        this.observationIntervalSeconds = benchmarkClient.averageObservationIntervalSeconds;
        this.observationIntervalVariabilityPct = TimeSeriesBenchmarker.OBSERVATION_INTERVAL_VARIABILITY_PCT;
    }

    /**
     * Protected access to 'isRunning' for use by the benchmark client
     * @return
     */
    boolean isRunning() {
        return isRunning;
    }

    /**
     * Protected access to the actual run time of the thread, for use by the benchmark client
     * @return
     */
    long runTime(){
        return (isFinished || isRunning()) ? System.currentTimeMillis() - startTime : 0;
    }

    /**
     * Protected access to the update count for the thread, for use by the benchmark client
     * @return
     */
    public int getUpdateCount() {
        return updateCount;
    }

    public void run(){
        startTime = System.currentTimeMillis();
        Map<String,Long> nextObservationTimes = new HashMap<>();
        for(int i = 0; i< timeSeriesCountPerObject; i++){
            nextObservationTimes.put(randomTimeSeriesName(),0L);
        }
        isRunning = true;
        while(getSimulationTime() - startTime < runDurationSeconds * TimeSeriesBenchmarker.MILLISECONDS_IN_SECOND * accelerationFactor){
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
        isFinished = true;
        isRunning = false;
    }

    /**
     * Time may be 'speeded up' during our simulation via use of the acceleration factor paramter
     * This function returns the accelerated simulation time represented as a unix epoch, in milliseconds
     *
     * @return
     */
    private long getSimulationTime(){
        return startTime + (System.currentTimeMillis() - startTime) * accelerationFactor;
    }

    /**
     * Randomly generate the next observation time for a time series observation
     * @return
     */
    private long nextObservationTime(){
        int intervalSamplingGranularity = 1000;
        // Randomly vary the observation interval by +/- observationIntervalVariabilityPct
        // First generate the variability
        double observationVariationPct = (intervalSamplingGranularity * (100 - observationIntervalVariabilityPct)
                + 2 * observationIntervalVariabilityPct * randomNumberGenerator.nextInt(intervalSamplingGranularity + 1)) / intervalSamplingGranularity;
        // then apply it to the average interval. Convert to milliseconds and divide by 100 as we were working in pct terms
        return getSimulationTime() + (int)(observationVariationPct * observationIntervalSeconds * TimeSeriesBenchmarker.MILLISECONDS_IN_SECOND)/100;
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
