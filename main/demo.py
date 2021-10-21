
from __future__ import print_function

import sys
from random import random
from operator import add

from livy import LivySession
import textwrap

if __name__ == "__main__":
    """
        To run this Python script you need to install requirements.txt first.
     
    """

    config_dict = {
        'spark.app.name':'Streaming Log Processor App',
        'spark.jars.packages':'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview,org.apache.kafka:kafka-clients:2.3.1',
        'partition.assignment.strategy': 'range',
        'kafka.partition.assignment.strategy':'range',
    }
    

    

    LIVY_URL = "http://192.168.49.91:8998"

    CHECKPOINTS = "checkpoints/log_data"
    DATA_PATH = "data/log_data"


    with LivySession.create(url=LIVY_URL, spark_conf=config_dict, 
    py_files=['https://files.pythonhosted.org/packages/df/59/3f611ecca70bc91959e3e1ec325f7835d15cc35585af71dbc6c1123be48e/python-geoip-geolite2-2015.0303.tar.gz'],
    #jars=["https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar"],
    #archives=["https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar"]
    ) as session:
        code= textwrap.dedent(f"""
            from pyspark.sql.functions import from_json, col, to_date, row_number, lit
            from pyspark.sql.window import Window

            
            df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "my-kafka.default.svc.cluster.local:9092") \
            .option("partition.assignment.strategy", "range") \
            .option("kafka.partition.assignment.strategy", "range") \
            .option("subscribe", "log-messages") \
            .load()
  
  
            data_schema = StructType([
                StructField("a", IntegerType())
                
                ])
            
            partition_window = Window().orderBy(lit('1'))
            valid_log_line_cond = (col("host").isNotNull() 
                | col("host").isNotNull() 
                | col("method").isNotNull() 
                | col("path").isNotNull() 
                | col("code").isNotNull() 
                | col("size").isNotNull()) 

            tmp_df=df.dropDuplicates().select(
                from_json(col("value").cast("string")).alias("payload"),
                col("timestamp"), to_date("timestamp").alias("date"), 
                (row_number().over(w) % F.lit(5)).alias("part")).select(
                    col("payload.host").alias("host"), 
                    col("payload.method").alias("method"), 
                    col("payload.path").alias("path"), 
                    col("payload.code").alias("code").cast("short"), 
                    col("payload.size").alias("size").cast("int"), 
                    col("date"), col("part"), col("timestamp")
                ).filter(valid_log_line_cond)
                

            result = tmp_df.writeStream.format("orc").option("checkpointLocation", "{CHECKPOINTS}")\
                .option("{DATA_PATH}", "log_data.orc")
                .partitionBy("date", "part").start()
            
            result.awaitTermination()
            
    """)

        session.run(code)
