package io.github.ken_tune.aero_time_series;

import org.junit.Assert;
import org.junit.Test;

public class TimeSeriesSimulatorTest {
    private static final int SECONDS_IN_DAY = 24 * 60 * 60;
    @Test
    /**
     * Check our simulator returns random values with the expected drift and variance
     */
    public void checkMeanAndVarianceObserved(){
        int startingValue = 12;
        int dailyDriftPct = 10;
        int dailyVariancePct = 10;
        int iterationCount = 1000000;
        int timeIncrementSeconds = 15;
        TimeSeriesSimulator simulator = new TimeSeriesSimulator(dailyDriftPct,dailyVariancePct);
        double[] values = new double[iterationCount +1];
        values[0] = startingValue;
        for(int i=1;i<=iterationCount;i++){
            values[i] = simulator.getNextValue(values[i-1],timeIncrementSeconds);
        }
        // Calculate mean of differences
        double sumDiffs = 0;
        for(int i =1;i<=iterationCount;i++) sumDiffs+= (values[i] - values[i-1])/values[i-1];
        double empiricalMeanPct = 100 * sumDiffs / iterationCount;
        double expectedMeanPct = (double)dailyDriftPct * timeIncrementSeconds/SECONDS_IN_DAY;
        Assert.assertTrue(Utilities.valueInTolerance(expectedMeanPct,empiricalMeanPct,10));

        double sumDiffsSqd = 0;
        for(int i =1;i<=iterationCount;i++) sumDiffsSqd+= Math.pow((values[i] - values[i-1])/values[i-1],2);
        double empiricalVariancePct = 100 * Math.sqrt((sumDiffsSqd - iterationCount * Math.pow(empiricalMeanPct/100,2))/iterationCount);
        double expectedVariancePct = dailyVariancePct * Math.sqrt((double)timeIncrementSeconds/SECONDS_IN_DAY);
        Assert.assertTrue(Utilities.valueInTolerance(expectedVariancePct,empiricalVariancePct,10));
    }
}
