
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
        'kafka.partition.assignment.strategy':'range'
    }
    

    

    LIVY_URL = "http://192.168.49.91:8998"

    CHECKPOINTS = "checkpoints/log_data"
    DATA_PATH = "data/log_data"


    with LivySession.create(url=LIVY_URL, spark_conf=config_dict, 
    #jars=["https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar"],
    #archives=["https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar"]
    ) as session:
        code= textwrap.dedent(f"""
            
            df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "my-kafka.default.svc.cluster.local:9092") \
            .option("partition.assignment.strategy", "range") \
            .option("kafka.partition.assignment.strategy", "range") \
            .option("subscribe", "log-messages") \
            .load()
  
  
            result=df.select("*").selectExpr("cast(value as string)")
            res = result.writeStream.format("orc").option("checkpointLocation", "{CHECKPOINTS}")\
                .option("{DATA_PATH}", "log_data.orc").start()
            res.awaitTermination()
            
    """)

        session.run(code)
