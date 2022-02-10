package io.github.aerospike_examples.aero_time_series.benchmark;

import io.github.aerospike_examples.aero_time_series.TestConstants;
import io.github.aerospike_examples.aero_time_series.Utilities;
import io.github.aerospike_examples.aero_time_series.benchmark.TimeSeriesSimulator;
import org.junit.Assert;
import org.junit.Test;

public class TimeSeriesSimulatorTest {
    private static final int SECONDS_IN_DAY = 24 * 60 * 60;
    @Test
    /**
     * Check our simulator returns random values with the expected drift and variance, within a certain tolerance
     */
    public void checkMeanAndVarianceObserved(){
        int startingValue = 12;
        //double dailyDriftPct = 10;
        double dailyDriftPct = 1;
        double dailyVariancePct = 1;
        int iterationCount = 100;
        int timeIncrementSeconds = 12*60*60;
        int tolerancePct = 10;
        TimeSeriesSimulator simulator = new TimeSeriesSimulator(dailyDriftPct,dailyVariancePct, TestConstants.RANDOM_SEED);
        double[] values = new double[iterationCount +1];
        values[0] = startingValue;
        for(int i=1;i<=iterationCount;i++){
            values[i] = simulator.getNextValue(values[i-1],timeIncrementSeconds);
        }
        checkDailyDriftPct(values,dailyDriftPct,timeIncrementSeconds,tolerancePct);
        checkDailyVolatilityPct(values,dailyVariancePct,timeIncrementSeconds,tolerancePct);
    }

    /**
     * Check daily volatility versus a set of values simulated using that daily volatility - is the computed daily volatility within tolerancePct of the expected daily volatility
     * @param values - sample values
     * @param dailyVariancePct  - expected drift
     * @param timeIncrementSeconds - average time between observations
     * @param tolerancePct - tolerance for deviation of found value from expected value
     */
    static void checkDailyVolatilityPct(double[] values, double dailyVariancePct,int timeIncrementSeconds,int tolerancePct){
        double empiricalVariancePct = calculateDailyVolatilityPct(values,timeIncrementSeconds);
        Assert.assertTrue(Utilities.valueInTolerance(dailyVariancePct,empiricalVariancePct,tolerancePct));
    }

    /**
     * Private method to calculate the daily volatility of a set of time series values observed every timeIncrementSeconds
     * @param values - sample values
     * @param timeIncrementSeconds - average time between observations
     * @return calculated daily volatility as a percentage
     */
    private static double calculateDailyVolatilityPct(double[] values, int timeIncrementSeconds) {
        int iterationCount = values.length -1;
        double requiredMean = calculateDailyDriftPct(values,timeIncrementSeconds) * timeIncrementSeconds / (100 * SECONDS_IN_DAY);
        double sumDiffsSqd = 0;
        for(int i =1;i<=iterationCount;i++) sumDiffsSqd+= Math.pow( (values[i] - values[i-1])/(values[i-1]),2);
        return 100 * Math.sqrt(SECONDS_IN_DAY * (sumDiffsSqd - iterationCount * Math.pow(requiredMean,2))/(iterationCount * timeIncrementSeconds));
    }

    /**
     * Check mean drift versus a set of values simulated using that mean drift - is the computed drift within tolerancePct of the expected drift
     * Package level visibility for
     * @param values - sample values
     * @param dailyDriftPct - expected drift
     * @param timeIncrementSeconds - average time period between observations
     * @param tolerancePct - tolerance for deviation of found value from expected value
     */
    static void checkDailyDriftPct(double[] values, double dailyDriftPct, int timeIncrementSeconds,int tolerancePct) {
        double empiricalMeanPct = calculateDailyDriftPct(values,timeIncrementSeconds);
        Assert.assertTrue(Utilities.valueInTolerance(dailyDriftPct, empiricalMeanPct, tolerancePct));
    }

    /**
     * Private method to calculate the mean drift from a set of values
     * @param values series of values whose mean drift is being tested
     * @param timeIncrementSeconds - average time period between observations
     * @return calculated mean daily drift as a percentage
     */
    private static double calculateDailyDriftPct(double[] values, int timeIncrementSeconds) {
        int iterationCount = values.length - 1;
        // Calculate mean of differences
        double sumDiffs = 0;
        for (int i = 1; i <= iterationCount; i++) sumDiffs += (values[i] - values[i - 1]) / values[i - 1];
        return 100 * sumDiffs * SECONDS_IN_DAY / (iterationCount * timeIncrementSeconds);
    }
}
