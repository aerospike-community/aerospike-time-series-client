package io.github.aerospike_examples.aero_time_series;

/**
 * Useful functions
 */
public class Utilities {
    /**
     * Utility function to test a value is within tolerance
     * @param expectedValue expected value
     * @param actualValue actual value
     * @param tolerancePct acceptable tolerance as a percentage
     * @return boolean indicating whether value is within tolerance
     */
    public static boolean valueInTolerance(double expectedValue, double actualValue,double tolerancePct){
        return Math.abs((actualValue - expectedValue)/expectedValue) < tolerancePct / 100;
    }

    /**
     * Throw this error if we have parsing exceptions
     */
    public static class ParseException extends Exception{
        public ParseException(String message){
            super(message);
        }
    }

}
