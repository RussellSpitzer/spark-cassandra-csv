package com.datastax.sparkcsv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.sys.process._
//import com.datastax.driver.spark._
import com.datastax.spark.connector._
import java.io.File

/**
 * Created by russellspitzer on 6/13/14.
 */


case class Config(master: String = "dsetool sparkmaster".!!.trim, // Calls a subprocess to get the spark master
                  filename: String = "exampleCsv",
                  keyspace: String = "ks",
                  table: String = "tab",
                  mapping: String = "exampleMapping",
                  maxCores: Int = 1,
                  executorMemory: String = "2g",
                  verify: Boolean = false,
                  cassandraIp: String = "127.0.0.1"
                   )


object ExampleLoad {


  def main(args: Array[String]) {

    val ipReg = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}""".r

    val parser = new scopt.OptionParser[Config]("sparkcsvexample") {
      head("Spark CSV Loader", "1.0")
      arg[String]("filename") action { (arg, config) => config.copy(filename = arg)} text {
        "Filename to read, csv, ex.(file:///temp/file.csv). If no locator uri it provided will look in Hadoop DefaultFS (CFS on DSE)"
      }
      arg[String]("keyspace") action { (arg, config) => config.copy(keyspace = arg)} text {
        "Keyspace to save to"
      }
      arg[String]("table") action { (arg, config) => config.copy(table = arg)} text {
        "Table to save to"
      }
      arg[String]("mapping") action { (arg, config) => config.copy(mapping = arg)} text {
        "A file containing the names of the Cassandra columns that the csv columns should map to, comma-delimited"
      }
      arg[String]("master") optional() action { (arg, config) => config.copy(master = arg)} text {
        "Spark Address of Master Node, Default runs `dsetool sparkmaster` to find master"
      }
      arg[String]("cassandraIp") optional() action { (arg, config) => config.copy(cassandraIp = arg)} text{
        "Ip Address of Cassandra Server, Default uses Spark Master IP address"
      }
      opt[Int]('m', "maxcores") optional() action { (arg, config) => config.copy(maxCores = arg)} text {
        "Number of cores to use by this application"
      }
      opt[String]('x', "executormemory") optional() action { (arg, config) => config.copy(executorMemory = arg)} text {
        "Amount of memory for each executor (JVM Style Strings)"
      }
      opt[Unit]('v', "verify") optional() action { (_, config) => config.copy(verify = true)} text {
        "Run verification checks after inserting data"
      }

      help("help") text {
        "CLI Help"
      }
    }
    parser.parse(args, Config()) map { config =>
      println("SparkMaster: ",config.master)
      println("Cassandra IP: ",config.cassandraIp)
      val modConf = config.copy(cassandraIp = ipReg findFirstIn (config.master) match {
        case Some(ipReg) => ipReg
        case None => "127.0.0.1"
      }
      )
      loadCSV(modConf)
    } getOrElse {
      System.exit(1)
    }
  }

  def loadCSV(config: Config) {


    //Read in the mapping file
    val mappingString = scala.io.Source.fromFile(config.mapping).getLines.mkString
    val mappingArray = mappingString.split(",")

    val sparkconf = new SparkConf()
      .setMaster(config.master)
      .setAppName("SparkExample: Load CSV")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(Array(System.getProperty("user.dir") + "/target/scala-2.10/spark-csv-assembly-1.0.jar"))
      .set("spark.cores.max", config.maxCores.toString)
      .set("spark.executor.memory", config.executorMemory.toString)
      .set("cassandra.connection.host", config.cassandraIp)

    //Make a spark context
    val sc = new SparkContext(sparkconf)

    //Make a CassandraRDD for our target table
    val cassRDD = sc.cassandraTable(config.keyspace, config.table)

    //Make an RDD from a text file and split it on ','
    println(config.filename)
    val textFileRDD = sc.textFile(config.filename)
    val lineRDD = textFileRDD.map { line => line.split(",",mappingArray.length) }


    //Print quick diagnostic about what we are about to do
    println("About to do the following inserts")
    println(mappingArray.mkString("\t"))
    println(lineRDD.take(5).map {
      _.mkString("\t")
    }.mkString("\n"))
    println("...")

    val insertRDD = lineRDD.map{elementArray => CassandraRow.fromMap((mappingArray zip elementArray) toMap)}

    //Count the lines to check whether or not we have inserted as many lines as we had
    var csvLineCount = 0l
    var cassRDDCount = 0l
    if (config.verify) {
      csvLineCount = lineRDD.count
      cassRDDCount = cassRDD.count
    }

    //Save text file to cassandra
    insertRDD.saveToCassandra(config.keyspace, config.table)

    if (config.verify) {
      val rddNewCount = cassRDD.count()
      println(s"Lines in CSV File: $csvLineCount")
      println(s"Lines in Table Before Insert File: $cassRDDCount")
      println(s"Lines in Table After Insert File : $rddNewCount")
      if (rddNewCount - cassRDDCount != csvLineCount) {
        println("Some lines were either not added or were overwritten, checking inserted data")
        val insertRDDKV = insertRDD.map( row => (mappingArray.map{col => row.get[Any](col)}.mkString(","),1))
        val cassRDDKV = cassRDD.map( row => (mappingArray.map{col => row.get[Any](col)}.mkString(","),1))
        val missingRows = insertRDDKV.leftOuterJoin(cassRDDKV).filter( kv => kv._2._2.isEmpty)
        missingRows.collect.foreach( row => println("Not Found in C*",row._1.toString))
        val missingRowCount = missingRows.count
        println(s"Found $missingRowCount Missing Rows")
        missingRows.foreach( row => println(row._1.toString()))
      }
    }

  println("Finished")

  }
}
