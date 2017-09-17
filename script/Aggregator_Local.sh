#!/usr/bin/env bash
SPARK_BIN="/home/haxiaolin/work/fds-work/infra-client/bin/spark-submit"
cluster_name="hbase_cluster_name"
$SPARK_BIN/spark-submit \
    --class com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate.Aggregator\
    --master local \
    --deploy-mode client \
    --num-executors 5 \
    --driver-memory 2g \
    --executor-memory 4g \
    --executor-cores 2 \
    ../target/galaxy-fds-cleaner-spark-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --cluster_name ${cluster_name} \
    --fds_file_cleaner_base_path "hdfs://${cluster_name}/home/operator/fdscleaner/aggregator"
