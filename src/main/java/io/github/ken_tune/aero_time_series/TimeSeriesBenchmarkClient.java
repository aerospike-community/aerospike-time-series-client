package io.github.ken_tune.aero_time_series;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class TimeSeriesBenchmarkClient {
    // Seed for random number generation
    private static long RANDOM_SEED = 6760187239798559903L;
    private static Random randomNumberGenerator = new Random(RANDOM_SEED);

    public static final int DEFAULT_TIME_SERIES_NAME_LENGTH = 10;
    public static final int DEFAULT_TIME_SERIES_COUNT = 10;
    public static final int DEFAULT_AVERAGE_OBSERVATION_INTERVAL = 10;
    public static final int DEFAULT_AVERAGE_ACCELERATION_FACTOR = 1;

    private final int timeSeriesCount = DEFAULT_TIME_SERIES_COUNT;
    private final int averageObservationIntervalSeconds = DEFAULT_AVERAGE_OBSERVATION_INTERVAL;
    private final int accelerationFactor = DEFAULT_AVERAGE_ACCELERATION_FACTOR;
    private final int timeSeriesNameLength = DEFAULT_TIME_SERIES_NAME_LENGTH;

    public static void main(String[] args){
        TimeSeriesBenchmarkClient timeSeriesBenchmarkClient = new TimeSeriesBenchmarkClient();
    }

    public void run(){
        Map<String,Long> lastObservation = new HashMap<>();
        long now = System.currentTimeMillis();
        for(int i=0;i<timeSeriesCount;i++){
            lastObservation.put(randomTimeSeriesName(),nextObservationTime());
        }
        while(true){
            Iterator<String> timeSeriesNames = lastObservation.keySet().iterator();
            while(timeSeriesNames.hasNext()){

            }

        }

    }

    private long nextObservationTime(){
        return System.currentTimeMillis() + (int)(averageObservationIntervalSeconds*(1000+(0.1*randomNumberGenerator.nextInt(1000))));
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
