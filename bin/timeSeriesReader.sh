#!/bin/bash

BENCHMARKER_JAR_PATH="../benchmarker/target/benchmarker-*-SNAPSHOT-jar-with-dependencies.jar"

# Check java is installed
if [ -z $(which java) ]
then
	echo "Java not installed"
	echo "Java is required to run the time series benchmarker"
fi

if [ ! -e $BENCHMARKER_JAR_PATH ]
then
	echo "benchmarker-<VERSION>-SNAPSHOT-jar-with-dependencies.jar jar not found in ../target"
	echo "You need to run mvn package -Dmaven.test.skip"

	if [ -z $(which mvn)]
	then
		echo "You will need mvn installed to do this - doesn't look like it is"
	fi
	exit 1
fi

java -cp $BENCHMARKER_JAR_PATH io.github.aerospike_examples.timeseries.benchmarker.TimeSeriesReader "$@"
