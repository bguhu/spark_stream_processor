package demo

import org.apache.livy.{LivyClientBuilder}
import java.net.URI
import java.io.File

object Demo {
  def main(args: Array[String]) :Unit = {
    val LIVY_URL = "http://192.168.49.90:8998"

    val client = new LivyClientBuilder().setURI(new URI(LIVY_URL)).build
    val etl_jar = "/home/guhu/p1/spark_stream_processor/target/scala-2.11/spark-streaming_2.11-1.0.jar"

    try {
      System.err.printf("Uploading %s to the Spark context...\n", etl_jar)
      client.uploadJar(new File(etl_jar)).get

      //System.err.printf("Running LogProcessor with %d samples...\n", samples)
      client.submit(new LogProcessor()).get
      //System.out.println("LogProcessor is roughly: ")
    } finally client.stop(true)

  }
}
