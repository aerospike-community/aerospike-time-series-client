package io.github.ken_tune.time_series;

/**
 * Useful functions
 */
public class Utilities {
    /**
     * Utility function to test a value is within tolerance
     * @param expectedValue
     * @param actualValue
     * @param tolerancePct
     * @return
     */
    public static boolean valueInTolerance(double expectedValue, double actualValue,double tolerancePct){
        return Math.abs((actualValue - expectedValue)/expectedValue) < tolerancePct / 100;
    }

}
