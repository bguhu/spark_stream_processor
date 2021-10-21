
from __future__ import print_function

import sys
from random import random
from operator import add

from livy import LivySession
import textwrap

if __name__ == "__main__":
    """
        To run this Python script you need to install livy-python-api-*version*.tar.gz with
        easy_install first.
        python /pathTo/pi_app.py http://<livy-server>:8998 2
    """

    if len(sys.argv) != 3:
        print("Usage: pi_app <livy url> <slices>", file=sys.stderr)
        exit(-1)

    slices = int(sys.argv[2])
    samples = 100000 * slices

    config_dict = {
        'spark.app.name':'Test App',
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
        # Run some code on the remote cluster
        #res=session.download("result")
        # Retrieve the result
        #res.show()



    # # client = HttpClient(sys.argv[1])
    # client = HttpClient("http://192.168.49.91:8998", load_defaults=False, config_dict=config_dict)
    
    

    # def f(_):
    #     x = random() * 2 - 1
    #     y = random() * 2 - 1
    #     return 1 if x ** 2 + y ** 2 <= 1 else 0

    # def pi_job(context):
    #     count = context.sc.parallelize(range(1, samples + 1), slices).map(f).reduce(add)
    #     return 4.0 * count / samples

    # pi = client.submit(pi_job).result()

    # print("Pi is roughly %f" % pi)
    # client.stop(True)