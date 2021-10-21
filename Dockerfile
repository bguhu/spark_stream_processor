FROM python:3.8.5

RUN apt-get clean && apt-get update \
        && apt-get install -y build-essential unzip \
        && wget -O Spark-stream-processor.zip https://github.com/bguhu/spark_stream_processor/archive/main.zip \
        && unzip Spark-stream-processor.zip \
        && mv spark_stream_processor-main /spark_stream_processor \
        && rm Spark-stream-processor.zip \
        && cd spark_stream_processor \
        && pip install -r requirements.txt \
        && apt-get clean \
    && rm -rf /var/lib/apt/listss/* /tmp/* /var/tmp/* 

WORKDIR /spark_stream_processor

CMD ["python", "main/demo.py"]
