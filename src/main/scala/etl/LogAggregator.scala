package demo

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, from_json, udf, to_date, hour, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import java.util.Calendar
//{ArrayType, StructType}

//from pyspark.sql.functions import udf
//from pyspark.sql.window import Window
//from pyspark.sql.types import *
//from geoip import geolite2

import org.apache.livy.{Job, JobContext}

//import org.apache.spark.implicits._

class LogAggregator extends Job[Unit] {

  override def call(jc: JobContext): Unit = {

    val spark: SparkSession = jc.sparkSession()
    import spark.implicits._

    val df = spark.read.orc("/tmp/data/orc_log_data.orc")

    val current_hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)

    val filtering_statement = current_hour match {
      case 0 => "date == current_date() and hour == 23"
      case _ => "date == date_sub(current_date(),1) and hour == hour(current_timestamp())-1"
    }


    df.filter(filtering_statement)
      .repartition(1, col("date"))
      .write.format("orc")
      .mode("append")
      .option("path", "/tmp/daily_aggregated_data/orc_log_data.orc")
      .partitionBy("date").save()

  }
}