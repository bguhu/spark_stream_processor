name := "spark-streaming"

version := "1.0"

scalaVersion := "2.11.12"

fork := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.6"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.6"

// Needed for structured streams
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.6"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.0.1"

//libraryDependencies += "info.batey.kafka" % "kafka-unit" % "0.7"

//libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "5.0.2"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"

libraryDependencies += "org.apache.livy" % "livy-client-http" % "0.7.1-incubating"


scalacOptions += "-target:jvm-1.8"


