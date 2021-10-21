
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
    #,org.apache.kafka:kafka-clients:2.0.0
    config_dict = {
        'spark.app.name':'StreamingApp',
        'spark.jars.packages':'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6,org.apache.kafka:kafka-clients:2.0.0',
        'partition.assignment.strategy': 'roundrobin',
        'kafka.partition.assignment.strategy':'roundrobin',
        'spark.driver.extraClassPath': '/opt/spark/jars/kafka-clients-2.0.0.jar',
        'spark.executor.extraClassPath': '/opt/spark/jars/kafka-clients-2.0.0.jar'
    }
    

    

    LIVY_URL = "http://192.168.49.92:8998"

    CHECKPOINTS = "checkpoints/log_data"
    DATA_PATH = "data/log_data"


    with LivySession.create(url=LIVY_URL, spark_conf=config_dict, 
        # https://pypi.org/project/python-geoip-geolite2 python package
        py_files=['https://files.pythonhosted.org/packages/df/59/3f611ecca70bc91959e3e1ec325f7835d15cc35585af71dbc6c1123be48e/python-geoip-geolite2-2015.0303.tar.gz'],
        #jars=["https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.6/spark-sql-kafka-0-10_2.11-2.4.6.jar",
        #"https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.0.0/kafka-clients-2.0.0.jar"]
    ) as session:
        code= textwrap.dedent(f"""
            from pyspark.sql.functions import from_json, col, to_date, hour, lit
            from pyspark.sql.window import Window
            from pyspark.sql.types import *
            from geoip import geolite2

            # udf method for converting ip to country code
            def get_country_code(ip:str)->str:
                if ip is not None:
                    match = geolite2.lookup(ip)
                    if match is not None:
                        return match.country
                return None

            udf_country_code = udf(lambda data: get_country_code(data))

            # Init DataStream and load DataFrame from
            df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "my-kafka.default.svc.cluster.local:9092") \
            .option("subscribe", "log-messages") \
            .load()

            # Construct the json part of the values received from kafka 
            schema = StructType([
                StructField("host", StringType()),
                StructField("user", StringType()),
                StructField("method", StringType()),
                StructField("path", StringType()),
                StructField("code", IntegerType()),
                StructField("size", IntegerType()),
                StructField("referer", StringType()),
                StructField("agent", StringType())])
            
  
            # to reach optimal partitioning size of 20-30gb per orc file, we use this window
            # to add a partitioning column
            # partition_window = Window().orderBy(lit('1'))
            # # to filter non valid log lines
            nonempty_log_line_cond = (col("host").isNotNull() 
                | col("method").isNotNull() 
                | col("path").isNotNull() 
                | col("code").isNotNull() 
                | col("size").isNotNull()) 

            tmp_df=df.select(
                    # convert kafka data payload from json to structtype
                    from_json(col("value").cast("string"), schema).alias("payload"),
                    # select both timestamp and date columns (date is used to partition on daily basis)
                    col("timestamp"), to_date("timestamp").alias("date"), 
                ).select(
                    udf_country_code(col("payload.host")).alias("country_code"), 
                    col("payload.method").alias("method"), 
                    col("payload.path").alias("path"), 
                    col("payload.code").alias("code"), 
                    col("payload.size").alias("size"), 
                    col("date"), col("timestamp"), hour(col("date")).alias("hour")
                ).filter(nonempty_log_line_cond)
                
           
            result = tmp_df.writeStream.outputMode('append').format("orc")\
                .option("checkpointLocation",  "checkpoints") \
                .option("path", "/tmp/orc_log_data.orc") \
                .partitionBy("date", "hour").start()

            result.awaitTermination()
            
    """)
        
        session.run(code)
