package structured

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import util.{EmbeddedKafkaServer, SimpleKafkaClient}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf, from_json, udf, to_date, hour, lit }
import org.apache.spark.sql.types._
//{ArrayType, StructType}

//from pyspark.sql.functions import udf
//from pyspark.sql.window import Window
//from pyspark.sql.types import *
//from geoip import geolite2

/**
    * signal that the partition/batch combination has been completely processed.
  */
object LogProcessor {

  def main (args: Array[String]) {

    val topic = "topic"


    val spark = SparkSession
      .builder
      .appName("LogProcessor")
      .getOrCreate()

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "my-kafka.default.svc.cluster.local:9092")
      .option("subscribe", "log-messages")
      .option("startingOffsets", "earliest") // equivalent of auto.offset.reset which is not allowed here
      .load()

    nonempty_log_line_cond = (col("host").isNotNull()
      | col("method").isNotNull()
      | col("path").isNotNull()
      | col("code").isNotNull()
      | col("size").isNotNull())

    tmp_df = ds1.select(
            //convert kafka data payload from json to structtype
            from_json(col("value").cast("string"), schema).alias("payload"),
            //select both timestamp and date columns
            //(date is used to partition on daily basis)
            col("timestamp"), to_date("timestamp").alias("date"),
        ).select(
            col("payload.host").alias("host"),
            col("payload.method").alias("method"),
            col("payload.path").alias("path"),
            col("payload.code").alias("code"),
            col("payload.size").alias("size"),
            col("date"), col("timestamp"), hour(col("date")).alias("hour")
        ).filter(nonempty_log_line_cond)


    // process the stream using a custom ForeachWriter that simply prints the data and the state of the ForeachWriter
    // in order to illustrate how it works
    val transfotmation_and_saving = tmp_df.writeStream
      .foreachRDD(rdd => {
        val df = rdd.toDF()
        //val dataFrame = sqlContext.read.json(rdd.map(_._2))
      })

    transfotmation_and_saving.start()
    query.awaitTermination()
    spark.stop()

    println("*** done")
  }
}