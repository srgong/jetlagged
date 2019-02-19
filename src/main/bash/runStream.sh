#!/bin/bash

spark-submit --class processing.FareStreamer \
  --master spark://<master_node>:7077 \
  --deploy-mode client \
  --conf spark.hdfs.in=hdfs://<abs_path>:9000 \
  --conf spark.kafka.producer=<worker_name>:9092 \
  --conf spark.kafka.topic=<topic_name> \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
 hdfs://<master_node>:9000/jetlagged-assembly-1.0.jar
