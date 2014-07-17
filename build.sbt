import AssemblyKeys._

name := "spark-csv"

version := "1.0"

organization := "com.datastax"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.6" % "test"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1" % "provided"

//Just using this to pull in all the other spark-driver dependencies
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-beta2"
//Cassandra Spark Driver is located in Lib Dir, Compiled before maven art.

resolvers += Resolver.sonatypeRepo("public")

//We do this so that Spark Dependencies will not be bundled with our fat jar but will still be included on the classpath
//When we do a sbt/run
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assemblySettings


