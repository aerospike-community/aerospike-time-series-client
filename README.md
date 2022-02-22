# Aerospike Time Series API	

## Introduction

Aerospike is a high performance distributed database, particularly well suited for real time transactional processing. It is aimed at institutions and use-cases that need high throughput ( 100k tps+), with low latency (95% completion in <1ms), while managing large amounts of data (Tb+) with 100% uptime, scalability and low cost.

Conceptually, Aerospike is most readily categorised as a key value database. In reality however it has a number of bespoke features that make it capable of supporting a much wider set of use cases. A good example is our [document API](https://aerospike.com/blog/aerospike-document-api/) which builds on our [collection data types](https://docs.aerospike.com/guide/data-types/cdt) in order to provide [JsonPath](https://goessner.net/articles/JsonPath/) support for documents.

Another general use case we can consider is support for time series. The combination of [buffered writes](https://docs.aerospike.com/architecture/storage#ssdflash) and efficient [map operations](https://docs.aerospike.com/guide/data-types/cdt-map) allows us to optimise for both read and write of time series data. This API leverages these features to provide a general purpose interface for efficient reading and writing of time series data at scale. Also included is a benchmarking tool allowing performance to be measured.

## Time Series Data

Time series data can be thought of as a sequence of observations associated with a given property of a single subject. An observation is a quantity comprising two elements - a timestamp and a value. A property is a measurable attribute such as speed, temperature, pressure or price. We can see then that examples of time series might be the speed of a given vehicle; temperature readings at a fixed location; pressures recorded by an industrial sensor or the price of a stock on a given exchange. In each case the series consists of the evolution of these properties over time.

A time series API in its most basic form needs to consist of

1) A function allowing the writing of time series observations
2) A function allowing the retrieval of time series observations

Additional conveniences might include

3. The ability to write data in bulk (batch writes)
4. The ability to query the data e.g. calculate the average, maximum or minimum.

## Time Series API

The Aerospike API provides the above via the TimeSeriesClient object. The API is as follows

```java
// Store a single data point for a named time series
void put(String timeSeriesName,DataPoint dataPoint);

// Store a batch of data points for a named time series
void put(String timeSeriesName, DataPoint[] dataPoints);

// Retrieve all data points observed between startDateTime and endDateTime for a named time series
DataPoint[] getPoints(String timeSeriesName,Date startDateTime, Date endDateTime);

// Retrieve the observation made at time dateTime for a named time series
DataPoint getPoint(String timeSeriesName,Date dateTime);

// Execute TimeSeriesClient.QueryOperation versus the observations recorded for a named time series
// recorded between startDateTime and endDateTime
// The operations may be any of COUNT, AVG, MAX, MIN or VOL (volatility)
double runQuery(String timeSeriesName, TimeSeriesClient.QueryOperation operation, Date fromDateTime, Date toDateTime);

```

A DataPoint is a simple object representing an observation and the time at which it was made, constructed as follows. The Java Date timestamp allows times to be specified to millisecond accuracy

```java
DataPoint(Date dateTime, double value)
```

## Simple Example

The code example below shows us inserting a series of 24 temperature readings, taken in Trafalgar Square, London, on the 14th February 2022. We give the time series a meaningful name - subject/property/units.

```java
// Let's store some temperature readings taken in Trafalgar Square, London. Readings are Centigrade.
String timeSeriesName = "TrafalgarSquare-Temperature-Centigrade";
// The readings were taken on the 14th Feb, 2022
Date observationDate = new SimpleDateFormat("yyyy-MM-dd").parse("2022-02-14");
// ... and here they are
double[] hourlyTemperatureObservations =
	new double[]{2.7,2.3, 1.9, 1.8, 1.8, 1.7, 2.3, 3.2, 4.7, 5.4, 6.3, 7.7, 7.9, 9.9, 9.3, 
               9.6, 9.7, 8.4, 7.4, 6.8, 5.5, 5.4, 4.3, 4.2};

// To store, create a time series client object. Requires AerospikeClient object and Aerospike namespace name
// new TimeSeriesClient(AerospikeClient asClient, String asNamespaceName)
TimeSeriesClient timeSeriesClient = new TimeSeriesClient(asClient,asNamespaceName);
// Insert our hourly temperature readings
for(int i=0;i<hourlyTemperatureObservations.length;i++){
  // The datapoint consists of the base date + the required number of hours
  DataPoint dataPoint = new DataPoint(
    Utilities.incrementDateUsingSeconds(observationDate,i * 3600),
    hourlyTemperatureObservations[i]);
  // Which we then 'put'
  timeSeriesClient.put(timeSeriesName,dataPoint);
}
```

As a diagnostic, we can get some basic information about the time series

```java
TimeSeriesInfo timeSeriesInfo = TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient,timeSeriesName);
System.out.println(timeSeriesInfo);

```

which will give

```console
Name : TrafalgarSquare-Temperature-Centigrade Start Date : 2022-02-14 00:00:00.000 End Date 2022-02-14 23:00:00.000 Data point count : 24

```

Another diagnostic allows the time series to be printed to the command line

```java
timeSeriesClient.printTimeSeries(timeSeriesName);
```
gives
```
Timestamp,Value
2022-02-14 00:00:00.000,2.70000
2022-02-14 01:00:00.000,2.30000
2022-02-14 02:00:00.000,1.90000
...
2022-02-14 22:00:00.000,4.30000
2022-02-14 23:00:00.000,4.20000
```

Finally we can run a basic query

```java
System.out.println(
  String.format("Maximum temperature is %.3f",
                timeSeriesClient.runQuery(timeSeriesName,
                TimeSeriesClient.QueryOperation.MAX,
                timeSeriesInfo.getStartDateTime(),timeSeriesInfo.getEndDateTime())));
```

```
Maximum temperature is 9.900
```

Note we could alternatively have used the batch call

```java
// Create an array of DataPoints
DataPoint[] dataPoints = new DataPoint[hourlyTemperatureObservations.length];
// Add our observations to the array
for (int i = 0; i < hourlyTemperatureObservations.length; i++) {
  // The datapoint consists of the base date + the required number of hours
  dataPoints[i] = new DataPoint(
    Utilities.incrementDateUsingSeconds(observationDate, i * 3600),
    hourlyTemperatureObservations[i]);
}
// Put the points in a single call
timeSeriesClient.put(timeSeriesName,dataPoints);
```

## Implementation

There are two key implementation concepts to grasp. Firstly, rather than store each data point as a separate object, they are inserted into Aerospike maps. Schematically, each time series object looks something like

```
{
	timestamp001 : value001,
	timestamp002 : value002,
	...
}
```

The maps must not grow to an indefinite extent, so the API ensures that each map will not grow beyond a specified maximum. By default this limit is 1000 points (represented by ``Constants.DEFAULT_MAX_ENTRIES_PER_TIME_SERIES_BLOCK``), although this can be altered. For sizing and performance implications of choice of this value, see elsewhere in this README.

The second implementation point follows on from the first. As there is a limit to the number of points that can be stored in a block, we need to have some mechanism for creating new blocks and keeping track of existing blocks for each time series. This is done, on a per time series basis, by maintaining an index of all blocks created. Conceptually this looks something like the following

```
{
	TimeSeriesName : "MyTimeSeries",
  ListOfDataBlocks : {
 		StartTimeForBlock1 : {EndTime: <lastTimeStampForBlock1>, EntryCount: <entriesInBlock1>},
 		StartTimeForBlock1 : {EndTime: <lastTimeStampForBlock1>, EntryCount: <entriesInBlock1>},
    ...
  }
}
```

## Additional Control

### Time Series Set Name

The Time Series Client will store all time series data in the same [Aerospike Set](https://docs.aerospike.com/operations/manage/sets). By default this is ```Constants.DEFAULT_TIME_SERIES_SET``` which has the value ``TimeSeriesSet``. A different set can be used by initialising the ```TimeSeriesClient``` using the four argument constructor

```TimeSeriesClient(AerospikeClient asClient, String asNamespace, String timeSeriesSet, int maxBlockEntryCount) ```

The index data is stored in a separate set whose name is formed by adding the suffix "idx" to the time series set name.

### Data Points per block

As per the *Implementation* section, the number of data points per time series object is capped. The default limit is 1000, but this can be changed via the ```maxBlockEntryCount``` constructor argument above. Note that this can be changed dynamically i.e. a new limit can be made use of simply by creating a new `TimeSeriesClient` object. This will not change the sizes of any blocks written previously, it will simply put in place a new upper limit.

### Read / Write policies

Aerospike allows a great deal of fine grained control of behaviour around availability issues, timeouts, commit levels and more via use of [Policy](https://docs.aerospike.com/guide/policies) objects. The TimeSeriesClient allows a similar level of control. By default, read and write policies are inherited from the AerospikeClient object used in the constructor. This behaviour can be overridden via the setter methods `setReadPolicy` and `setWritePolicy`.

## Sizing

Empirically, the storage requirement per data point was found to be 17.33 bytes per data point, having inserted 8.64m data points (one per second, over a 24 hour period for 10 time series). As above, by default there will be one object per 1000 data points by default, although this value can be changed by the user.

The index object requires 42 bytes per entry. In theory this imposes an upper limit on the number of entries per time series as Aerospike has an upper limit per object of 1mb by default - see [write-block-size](https://docs.aerospike.com/reference/configuration#write-block-size). The implication is that the maximum number of index entries is 23,800. With a default max entry count of 1000, this implies a limit of 23.8m points per time series at the time of writing. Some options are available however. Firstly, the write-block-size can be increased to a maximum value of 8mb. Secondly, the max entry count value can be increased. Thirdly, this limit may be addressed in a future release.

## Performance

The throughput (max number of points that can be written per second) is fundamentally limited by the throughput the underlying disks will support. It should be noted that when a point is written to a block, the full block is re-written. Increasing the max entry count per block will result in larger writes for each data point, ultimately reducing the amount of throughput.

On the other hand, reading of a data point block is a single read, and for each read you get maxBlockEntryCount data points. So by increasing this value you improve your read rate.

The selected default value of 1000 points per block is a good compromise therefore. It results in ~16k object sizes.

Our [ACT](https://docs.aerospike.com/operations/plan/ssd/ssd_certification) method for rating disks can be made use of to determine time series performance. To get the number of 1.5kb reads or updates supported by a device divide the ACT rating by 3 (this because an update is a read and a write). A 300k device such as the  Intel P4610 will then support at least 100 * 1.5 / 16 = 9300 writes per second and 9300 reads per second. The read and write rates needed can be supported by linearly scaling the devices as needed. In practice these numbers can probably be bettered - see later. 

Aerospike has an upper limit on the number of updates per object of approximately 100. It is recommended that the frequency of updates per series per second is therefore no more than 20.

## Benchmarking

The Time Series API ships with a benchmarking tool. Three modes of operation are provided - real time insert, batch insert and query.

To make use of the benchmarker, download **this** repository and run `mvn assembly:single` to compile. Java 8+ and maven are required.

```bash
git clone https://github.com/aerospike-examples/aerospike-time-series-client.git
cd aerospike-time-series-client
mvn assembly:single
```

The benchmarker is found in the project's *bin* directory. Usage is as follows

```bash
./timeSeriesBenchmarker.sh

-a,--acceleration <arg>      Simulation acceleration factor (clock speed multiplier). Only valid in realTimeWrite mode. Optional.

-b,--recordsPerBlock <arg>   Max time series points in each Aerospike object. Optional. Defaults to 1000

-c,--timeSeriesCount <arg>   No of time series to simulate. Optional.Defaults to 100

-d,--duration <arg>          Simulation duration in seconds. Required for realTimeWrite and query mode. Not valid in batchInsert mode

-h,--host <arg>              Aerospike seed host. Required

-m,--mode <arg>              Benchmark mode - values allowed are realTimeWrite, batchInsert and query. Required.

-n,--namespace <arg>         Namespace for time series. Required.

-p,--interval <arg>          Average interval between observations. Required

-r,--timeSeriesRange <arg>   Period to be spanned by time series. Only valid in batchInsert mode. Specify as <number><unit> where <unit is one of Y(ears),D(ays),H(ours),M(inutes),S(econds) e.g. 1Y or 12H

-s,--set <arg>               Set for time series. Defaults to TimeSeries

-z,--threads <arg>           Thread count required. Optional. Defaults to 1
```

### Real Time Benchmarking

As a simple example, let's insert 10 seconds of data for a single time series, with observations being made once per second.

```bash
./timeSeriesBenchmarker.sh -h <AEROSPIKE_HOST_IP>  -n <AEROSPIKE_NAMESPACE> -m realTimeWrite -p 1 -c 1 -d 10
```

Sample output

```
Aerospike Time Series Benchmarker running in real time insert mode

Updates per second : 1.000
Updates per second per time series : 1.000

In real time benchmark we prime blocks so they don't all fill at the same time. Pct complete 0.000%
In real time benchmark we prime blocks so they don't all fill at the same time. Pct complete 100.000%

Run time : 0 sec, Update count : 1, Current updates/sec : 1.029, Cumulative updates/sec : 1.027
Run time : 1 sec, Update count : 2, Current updates/sec : 1.000, Cumulative updates/sec : 1.013
Run time : 2 sec, Update count : 2, Current updates/sec : 0.000, Cumulative updates/sec : 0.672
...
Run time : 8 sec, Update count : 9, Current updates/sec : 1.000, Cumulative updates/sec : 1.003
Run time : 9 sec, Update count : 10, Current updates/sec : 1.000, Cumulative updates/sec : 1.003

Run Summary

Run time : 10 sec, Update count : 10, Cumulative updates/sec : 0.997

```

We can make use of another utility to see the output - ./timeSeriesReader.sh - also found in the bin directory. This can be run for a named time series, or alternatively, will select a time series at random.

```
usage: TimeSeriesReader
 -h,--host <arg>             Aerospike seed host. Required
 -i,--timeSeriesName <arg>   Name of time series
 -n,--namespace <arg>        Namespace for time series. Required.
 -s,--set <arg>              Set for time series. Defaults to TimeSeries
```

Here is sample output for our simple example

```
./timeSeriesReader.sh -h <AEROSPIKE_HOST_IP>  -n <AEROSPIKE_NAMESPACE>
Running TimeSeriesReader

No time series specified - selecting series AFNJFKSKDV

Name : AFNJFKSKDV Start Date : 2022-02-22 12:17:13.294 End Date 2022-02-22 12:17:23.185 Data point count : 11

Timestamp,Value
2022-02-22 12:17:13.294,97.37854
2022-02-22 12:17:14.247,97.34929
2022-02-22 12:17:15.263,97.33103
...
2022-02-22 12:17:22.212,97.31197
2022-02-22 12:17:23.185,97.29315

```

We can see that we have had sample points generated over a ten second period, with the series given a random name.

The benchmarker can be run at greater scale using the -c (time series count) flag. You may also wish to make use of -z (multi-thread) flag in order to acheive the required throughput. The benchmarker will warn you if the required throughput is not being achieved.

Another real time option is acceleration via the -a flag. This runs the simulation at an accelerated rate. So for instance if you wished to insert points every 30 seconds over a 1 hour period (120 points), you could shorten the time of the run by running using '-a 30'. This will 'speed up' the simulation by a factor of 30, so it will only take 120s. A higher number would also be possible. The benchmarker will indicate the actual update rates. For example

```
./timeSeriesBenchmarker.sh -h <AEROSPIKE_HOST>  -n <AEROSPIKE_NAMESPACE> -m realTimeWrite -c 5 -p 10 -a 10 -d 10
Aerospike Time Series Benchmarker running in real time insert mode

Updates per second : 5.000
Updates per second per time series : 1.000
```

You will notice messages similar to the following when real time benchmarking is started - particularly if the time series count is high.

```
In real time benchmark we prime blocks so they don't all fill at the same time. Pct complete 11.231%
In real time benchmark we prime blocks so they don't all fill at the same time. Pct complete 17.649%
```

The reason for this is, by default, the 'blocks' will all fill up at the same time when using the real time benchmarked. This will create a saw-tooth like load on the underlying disks. To avoid this, when running in real time benchmark mode, for each series, the first block is initialised with a random number of records so the 'filling up' of blocks happens at a uniform rate. This is the 'priming' that is referred to. The 'dummy' records are removed at the end of the simulation.

### Batch Insertion

A disadvantage of the 'real time' benchmarker is precisely that - the loading occurs in real time. You may wish to build your sample time series as quickly as possible. The batch insert mode is provided for this purpose.

In this mode, data points are loaded a block at a time, effectively as fast as the benchmarker will run. The invocation below will create 1000 sample series (-c flag), over a period of 1 year (-r flag), with 30 seconds between each observation.

```
./timeSeriesBenchmarker.sh -h <AEROSPIKE_HOST_IP>  -n <AEROSPIKE_NAMESPACE>  -m batchInsert -c 10 -p 30 -r 1Y 
```

```
./timeSeriesBenchmarker.sh -h $HOST  -n test  -m batchInsert -c 1000 -p 30 -r 1Y -z 100 

Aerospike Time Series Benchmarker running in batch insert mode

Inserting 1051200 records per series for 1000 series, over a period of 31536000 seconds

Run time : 0 sec, Data point insert count : 0, Effective updates/sec : 0.000. Pct complete 0.000%
Run time : 1 sec, Data point insert count : 1046000, Effective updates/sec : 870216.306. Pct complete 0.100%
Run time : 2 sec, Data point insert count : 2568000, Effective updates/sec : 1146363.231. Pct complete 0.244%
Run time : 3 sec, Data point insert count : 4196000, Effective updates/sec : 1308796.007. Pct complete 0.399%
Run time : 4 sec, Data point insert count : 5806000, Effective updates/sec : 1372576.832. Pct complete 0.552%
...
Run time : 577 sec, Data point insert count : 1051077000, Effective updates/sec : 1820986.414. Pct complete 99.988%
Run time : 578 sec, Data point insert count : 1051158000, Effective updates/sec : 1817977.108. Pct complete 99.996%

Run Summary

Run time : 578 sec, Data point insert count : 1051200000, Effective updates/sec : 1816538.588. Pct complete 100.000%
```

### Query Benchmarking

Having two different methods for generating data now puts us in the position where we can consider query benchmarking. This is the third and final aspect of the benchmarking toolkit.

Query benchmarking can be invoked via the 'query' mode. We choose how long to run the benchmarker for (-d flag) and the number of threads to use (-z flag).

What the benchmarker does, is scans the database to determine all time series available. Each iteration of the benchmarker selects a series at random and calculates the average value of the series. The necessitates pulling all data points for the series to the client side and doing the necessary calculation. We can ensure the queries are consistent in terms of magnitude by using the batch insert aspect of the benchmarker.

Sample invocation and output

```
^C[ec2-user@ip-10-0-0-158 bin]$ ./timeSeriesBenchmarker.sh -h $HOST -n test -m query -z 1 -d 120
Aerospike Time Series Benchmarker running in query mode

Time series count : 956, Average data point count per query 1051200

Run time : 0 seconds, Query count : 0, Current queries per second 0.000, Current latency 0.000s, Avg latency 0.000s, Cumulative queries per second 0.000
Run time : 1 seconds, Query count : 1, Current queries per second 1.004, Current latency 0.682s, Avg latency 0.682s, Cumulative queries per second 1.000
Run time : 2 seconds, Query count : 3, Current queries per second 2.002, Current latency 0.579s, Avg latency 0.613s, Cumulative queries per second 1.500
Run time : 3 seconds, Query count : 4, Current queries per second 1.000, Current latency 0.773s, Avg latency 0.653s, Cumulative queries per second 1.333
Run time : 4 seconds, Query count : 6, Current queries per second 2.000, Current latency 0.574s, Avg latency 0.627s, Cumulative queries per second 1.500
Run time : 5 seconds, Query count : 8, Current queries per second 2.000, Current latency 0.497s, Avg latency 0.595s, Cumulative queries per second 1.600
Run time : 6 seconds, Query count : 9, Current queries per second 1.000, Current latency 0.653s, Avg latency 0.601s, Cumulative queries per second 1.500
Run time : 7 seconds, Query count : 11, Current queries per second 2.000, Current latency 0.636s, Avg latency 0.607s, Cumulative queries per second 1.571
Run time : 8 seconds, Query count : 13, Current queries per second 2.000, Current latency 0.508s, Avg latency 0.592s, Cumulative queries per second 1.625
...

```

## Simulation

It is helpful to simulate time series data realistically. The Time Series API contains a *TimeSeriesSimulator* class to help. This is made use of by the Benchmarker classes and may also be used independently.

Many time series over a short period at least, follow a [Brownian Motion](https://en.wikipedia.org/wiki/Brownian_motion). The *TimeSeriesSimulator* allows this to be simulated. The idea is that if we look at the *relative change* in our observed value, then the *expected* mean change should be proportional to the time between observations and the *expected variance* should similarly be proportional to the period in question. Formally, let X(t) be the observation of the subject property X at time &tau;. After a time t let the value of X be X(&tau;+t). The simulation distributes the value of (X(&tau; +t) - X(&tau;)) / X(&tau;) i.e. the relative change in X like a normal distribution with mean &mu;t and variance &sigma;<sup>2</sup>t.

<center>(X(t + &tau;) - X(t)) / X(t) ~ N(&mu;t,&sigma;<sup>2</sup>t.)</center>

We can use the simulator as follows

```java
// Initialise the simulator - daily drift is 2%, daily volatility is 5%
// Implies on average, over the course of a day, the value will increase by 2%
// and with ~70% probability the series will be between -3% and 7% of its original value
TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(2,5);
// Initial value
double seriesCurrentValue = 10;
// Time between observations
int timeBetweenObservations = 30;
// Ten iterations
for(int i = 0;i<=10;i++){
  // Print current value
	System.out.println(String.format(
    "Series value after %d seconds : %.5f",i*timeBetweenObservations,seriesCurrentValue));
  // Get next value
	seriesCurrentValue = timeSeriesSimulator.getNextValue(seriesCurrentValue,timeBetweenObservations);
}
```

Sample output

```
Series value after 0 seconds : 10.00000
Series value after 30 seconds : 10.00089
Series value after 60 seconds : 9.99232
...
Series value after 270 seconds : 10.01382
Series value after 300 seconds : 9.99846
```

### Benchmarker Simulation

The benchmarker uses the following values for daily drift and volatility

```java
public static final int DEFAULT_DAILY_VOLATILITY_PCT = 10;
public static final int DEFAULT_DAILY_DRIFT_PCT = 10;
```

Additionally some variability is introduced into the timing of the observations - the period between observations is varied by up to plus or minus the amount below.

```java
public static final int OBSERVATION_INTERVAL_VARIABILITY_PCT = 5;
```

Let's have a look at some representative output - every 5 minutes over a period of 1 day

```
./timeSeriesBenchmarker.sh -h 172.28.128.7 -n test -m batchInsert -c 1 -p 300 -r 1D 
```

Capture this using the reader

```
./timeSeriesReader.sh -h 172.28.128.7 -n test 
```

Sample output

```
Running TimeSeriesReader

No time series specified - selecting series PVLGMUDNKY

Name : PVLGMUDNKY Start Date : 2022-02-21 23:59:51.120 End Date 2022-02-22 23:54:52.027 Data point count : 288

Timestamp,Value
2022-02-21 23:59:51.120,13.44455
2022-02-22 00:04:45.720,13.38021
2022-02-22 00:09:56.460,13.37291
2022-02-22 00:14:55.110,13.42059
2022-02-22 00:20:01.020,13.49398
2022-02-22 00:24:54.240,13.37568
2022-02-22 00:29:49.260,13.41944
2022-02-22 00:35:03.750,13.42574
2022-02-22 00:39:50.220,13.41320
2022-02-22 00:45:00.510,13.46321
2022-02-22 00:49:58.020,13.59341
....
2022-02-22 23:34:59.227,15.36061
2022-02-22 23:39:50.467,15.36623
2022-02-22 23:44:50.497,15.35605
2022-02-22 23:49:40.387,15.33868
2022-02-22 23:54:52.027,15.37945
```

The above shows the deliberately introduced variability in the observation period.

Finally, the chart below, which was created by simply pulling the above data into Excel gives a sample of the qualitative nature of the data that is being generated.

![image-20220222164504014](/Users/ken/Library/Application Support/typora-user-images/image-20220222164504014.png)

