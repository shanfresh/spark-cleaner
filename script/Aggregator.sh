#!/usr/bin/env bash
SPARK_BIN="/Users/admin/Dev/spark2/bin"
processing_date = "2017-08-09"
$SPARK_BIN/spark-submit \
    --class com.xiaomi.infra.galaxy.fds.spakcleaner.job.Aggregator \
    --master local \
    --deploy-mode client \
    --num-executors 5 \
    --driver-memory 2g \
    --executor-memory 4g \
    --executor-cores 2 \
    ../target/galaxy-fds-cleaner-spark-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --date "${processing_date}" \
    --out_put_hdfs_file "hdfs://user/home/haxiaolin/spark-fds-cleaner/${processing_date}/Aggregator"
