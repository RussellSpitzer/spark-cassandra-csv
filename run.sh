#!/bin/bash
export SPARK_CLIENT_CLASSPATH=`pwd`/target/scala-2.10/spark-csv-assembly-1.0.jar
if [ ! -f $SPARK_CLIENT_CLASSPATH ]; then
    echo "Couldn't find $SPARK_CLIENT_CLASSPATH"
    exit 1
fi

exec dse spark-class com.datastax.sparkcsv.ExampleLoad "$@"
