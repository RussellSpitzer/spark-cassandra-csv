import AssemblyKeys._

name := "spark-csv"

version := "1.0"

organization := "com.datastax"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.6" % "test"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1" % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-beta1" % "provided"

//Creates jar to be distributed for the spark application
exportJars := true

resolvers += Resolver.sonatypeRepo("public")

//We do this so that Spark Dependencies will not be bundled with our fat jar but will still be included on the classpath
//When we do a sbt/run
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

//For building a stand-alone jar for DSE, this will include in scopt in the jar allowing dse spark-shell to run
//See run.sh for the addition of the assembly jar to the classpath and execution
assemblySettings


