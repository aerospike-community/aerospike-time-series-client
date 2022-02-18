#!/bin/bash

AERO_TIME_SERIES_JAR_LOCATION="../target/aero-time-series-client-*-SNAPSHOT-jar-with-dependencies.jar"
# Check java is installed
if [ -z $(which java)]
then
	echo "Java not installed"
	echo "Java is required to run the time series benchmarker"
fi

if [ ! -e $AERO_TIME_SERIES_JAR_LOCATION ]
then
	echo "aero-time-series-client-<VERSION>-SNAPSHOT-jar-with-dependencies.jar jar not found in ../target"
	echo "You need to run mvn assembly:single to build"

	if [ -z $(which mvn)]
	then
		echo "You will need mvn installed to do this - doesn't look like it is"
	fi
	exit 1
fi

java -jar $AERO_TIME_SERIES_JAR_LOCATION "$@"