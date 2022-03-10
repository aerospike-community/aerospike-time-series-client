package io.github.aerospike_examples.timeseries;

/**
 * Enumeration of the types of queries we can run against a time series
 */
public enum QueryOperation {

    /**
     * Indicates the MAX operation should be executed across an array of DataPoints
     */
    MAX("max", "maximum value of series"),

    /**
     * Indicates the MIN operation should be executed across an array of DataPoints
     */
    MIN("min", "minimum value of series"),

    /**
     * Indicates the AVG operation should be executed across an array of DataPoints
     */
    AVG("avg", "average value of series"),

    /**
     * Indicates the COUNT operation should be executed across an array of DataPoints
     */
    COUNT("count", "number of values in the series"),

    /**
     * Indicates the VOL operation ( = sqrt(variance) ) should be executed across an array of DataPoints
     */
    VOL("vol", "volatility of values in series");

    private final String shortName;
    private final String description;

    QueryOperation(String shortName, String description) {
        this.shortName = shortName;
        this.description = description;
    }

    /**
     * Get short name for operation
     *
     * @return short name for operation
     */
    @SuppressWarnings("unused")
    public String getShortName() {
        return shortName;
    }

    /**
     * Description of operation
     *
     * @return description of operation
     */
    @SuppressWarnings("unused")
    public String getDescription() {
        return description;
    }
}
