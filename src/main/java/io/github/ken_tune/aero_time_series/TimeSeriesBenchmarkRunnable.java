package io.github.ken_tune.aero_time_series;

import java.util.*;

// Implementation of individual runnable TimeSeriesBenchmark object
// Not intended for direct use - hence package level visibility
class TimeSeriesBenchmarkRunnable implements Runnable {

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
    private double observationIntervalVariabilityPct;
    // Randomness generation
    private Random random;


    // The simulator is used for generating the time series values
    private TimeSeriesSimulator simulator;

    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     * @param asHost - Aerospike Host
     * @param asNamespace - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     */
    TimeSeriesBenchmarkRunnable(String asHost, String asNamespace, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient){
        this(asHost,asNamespace,timeSeriesCountPerObject,benchmarkClient,new Random().nextLong());
    }


    /**
     * Constructor for a runnable that will generate timeSeriesCount time series for us
     * Package level visibility as this will not be used in isolation
     * @param asHost - Aerospike Host
     * @param asNamespace - Aerospike Namespace
     * @param timeSeriesCountPerObject - No of timeseries to generate
     * @param benchmarkClient - Initialise with a benchmarkClient object - some of the config is taken from this
     * @param randomSeed - initialise with a specific seed for deterministic results
     */
    TimeSeriesBenchmarkRunnable(String asHost, String asNamespace, int timeSeriesCountPerObject, TimeSeriesBenchmarker benchmarkClient, long randomSeed){
        timeSeriesClient = new TimeSeriesClient(asHost,asNamespace);
        this.timeSeriesCountPerObject = timeSeriesCountPerObject;
        this.runDurationSeconds = benchmarkClient.runDuration;
        this.accelerationFactor = benchmarkClient.accelerationFactor;
        this.timeSeriesNameLength = benchmarkClient.timeSeriesNameLength;
        this.observationIntervalSeconds = benchmarkClient.averageObservationIntervalSeconds;
        this.observationIntervalVariabilityPct = TimeSeriesBenchmarker.OBSERVATION_INTERVAL_VARIABILITY_PCT;
        this.simulator = new TimeSeriesSimulator(benchmarkClient.dailyDriftPct,benchmarkClient.dailyVolatilityPct,randomSeed);
        this.random = new Random(randomSeed);
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
        Map<String,Long> lastObservationTimes = new HashMap<>();
        Map<String,Long> nextObservationTimes = new HashMap<>();
        Map<String,Double> lastObservationValues = new HashMap<>();

        for(int i = 0; i< timeSeriesCountPerObject; i++){
            String timeSeriesName = randomTimeSeriesName();
            double observationValue = initTimeSeriesValue();
            lastObservationTimes.put(timeSeriesName,startTime);
            lastObservationValues.put(timeSeriesName,observationValue);
            timeSeriesClient.put(timeSeriesName,new DataPoint(new Date(startTime),observationValue));
            nextObservationTimes.put(timeSeriesName,nextObservationTime(startTime));
        }
        isRunning = true;
        while(getSimulationTime() - startTime < (long)runDurationSeconds * TimeSeriesBenchmarker.MILLISECONDS_IN_SECOND * accelerationFactor){
            Iterator<String> timeSeriesNames = nextObservationTimes.keySet().iterator();
            while(timeSeriesNames.hasNext()){
                String timeSeriesName = timeSeriesNames.next();
                long nextObservationTime = nextObservationTimes.get(timeSeriesName);
                if(nextObservationTime < getSimulationTime()) {
                    updateCount++;
                    double timeIncrement = (double)(nextObservationTime - lastObservationTimes.get(timeSeriesName))/TimeSeriesBenchmarker.MILLISECONDS_IN_SECOND;
                    double observationValue = simulator.getNextValue(lastObservationValues.get(timeSeriesName),timeIncrement);
                    timeSeriesClient.put(timeSeriesName,new DataPoint(new Date(nextObservationTime),observationValue));
                    lastObservationValues.put(timeSeriesName,observationValue);
                    lastObservationTimes.put(timeSeriesName,nextObservationTime);
                    nextObservationTimes.put(timeSeriesName,nextObservationTime(nextObservationTime));
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
    private long nextObservationTime(long lastObservationTime){
        int intervalSamplingGranularity = 1000;
        // Randomly vary the observation interval by +/- observationIntervalVariabilityPct
        // First generate the variability
        double observationVariationPct = 100  - observationIntervalVariabilityPct
                + (2 * observationIntervalVariabilityPct * random.nextInt(intervalSamplingGranularity + 1) / intervalSamplingGranularity);
        // then apply it to the average interval. Convert to milliseconds and divide by 100 as we were working in pct terms
        return lastObservationTime + (long)(observationVariationPct * observationIntervalSeconds * TimeSeriesBenchmarker.MILLISECONDS_IN_SECOND/100);
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
        for(int i=0;i<timeSeriesNameLength;i++) timeSeriesName[i] = availableCharacters[random.nextInt(availableCharacters.length)];
        return String.valueOf(timeSeriesName);
    }

    public double initTimeSeriesValue(){
        double TIME_SERIES_MIN_START_VALUE = 10.0;
        double TIME_SERIES_MAX_START_VALUE = 100.0;
        return TIME_SERIES_MIN_START_VALUE + random.nextDouble() * (TIME_SERIES_MAX_START_VALUE - TIME_SERIES_MIN_START_VALUE);
    }

}
