Spark CSV Loader for Cassandra
==============================

An Example Tool for Using Spark to load a CSV file into Cassandra using spark


    Spark CSV Loader 1.0
    Usage: sparkcsvexample [options] filename keyspace table mapping [master] [cassandraIp]
    
      filename
            Filename to read, csv, ex.(file:///temp/file.csv). If no locator uri it provided will look in Hadoop DefaultFS (CFS on DSE)
      keyspace
            Keyspace to save to
      table
            Table to save to
      mapping
            A file containing the names of the Cassandra columns that the csv columns should map to, comma-delimited
      master
            Spark Address of Master Node, Default runs `dsetool sparkmaster` to find master
      cassandraIp
            Ip Address of Cassandra Server, Default uses Spark Master IP address
      -m <value> | --maxcores <value>
            Number of cores to use by this application
      -x <value> | --executormemory <value>
            Amount of memory for each executor (JVM Style Strings)
      -v | --verify
            Run verification checks after inserting data
      --help
            CLI Help
            

This tool is designed to work with both standalone Apache Spark and Cassandra Clusters as well as DataStax
Cassandra/Spark Clusters.

Building the project
---------------------
To build go to the home directory of the project and run 

    ./sbt/sbt assembly
    
This will produce a fat-jar in `target/scala-2.10/spark-csv-assembly-1.0.jar`. Which needs to be included in any running 
Spark job. It contains the references to the anonymous functions which Spark will use when running.

Creating the Example Keyspace and Table
--------------------------------
This application assumes that the keyspace and table to be inserted to already exist. To create 
the table used in the example used below run the following commands in cqlsh.
   
    CREATE KEYSPACE ks WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': '1'
    };
    
    USE ks;
    
    CREATE TABLE tab (
      key int,
      data1 int,
      data2 int,
      data3 int,
      PRIMARY KEY ((key))
    )


Running with Datastax Enterprise
--------------------------------

When running on a Datstax Enterprise Cluster with Spark Enabled the app can be run with the included
run.sh script. This will include the fat-jar referenced above on the classpath for the dse spark-class call
and run the application. Running with this method will pickup your spark-env.sh file and correctly place the logs
in your predefined locations.

    ##example
    ./run.sh -m 4 file://`pwd`/exampleCsv ks tab exampleMapping
    
Running with Apache Cassandra
-------------------------------

We can run directly from sbt using

    #Note that here we need to specify the spark master uri and cassandra ip, otherwise
    #the program will try to use DataStax Enterprise to pick up these values
    ./sbt/sbt "run -m 4 file://`pwd`/exampleCsv ks tab exampleMapping spark://127.0.0.1:7077 127.0.0.1"    


