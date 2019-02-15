#!/bin/bash


 spark-submit --class FareSelector \
   --master spark://<master_node>:7077 \
   --deploy-mode client \
   --conf spark.kafka.brokers=<worker_node>:9092 \
   --conf spark.kafka.topic=<topic_name> \
   --conf spark.kafka.startingOffsets=latest \
   --conf spark.redis.host=<redis_host> \
   --conf spark.redis.port=6379 \
   --conf spark.driver.memory=2g \
   --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  hdfs://<master_node>:9000/jetlagged-assembly-1.0.jar
