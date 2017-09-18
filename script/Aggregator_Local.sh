SPARK_BIN="/home/haxiaolin/infra-client/bin"
HBASE_CLUSTER_NAME="c4tst-galaxy-staging"
YARN_CLUSTER_NAME="c4tst-staging"
$SPARK_BIN/spark-submit \
    --cluster ${HBASE_CLUSTER_NAME}  \
    --class com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate.Aggregator \
    --master yarn-cluster \
    --hbase "hbase://${HBASE_CLUSTER_NAME}" \
    --num-executors 3 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --properties-file local.conf \
    galaxy-fds-spark-cleaner-1.0-SNAPSHOT.jar \
    --fds_file_cleaner_base_path "hdfs://${YARN_CLUSTER_NAME}/home/operator/fdscleaner/aggregator/" \
    --hbase_cluster_name ${HBASE_CLUSTER_NAME} \
    --yarn_cluster_name ${YARN_CLUSTER_NAME}
