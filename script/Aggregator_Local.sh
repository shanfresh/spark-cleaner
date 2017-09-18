SPARK_BIN="/home/haxiaolin/infra-client/bin"
hbase_cluster_name="c4tst-galaxy-staging"
yarn_cluster_name="c4tst-staging"
$SPARK_BIN/spark-submit \
    --cluster ${yarn_cluster_name}  \
    --class com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate.Aggregator \
    --master yarn-cluster \
    --hbase "hbase://${hbase_cluster_name}" \
    --num-executors 3 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --properties-file local.conf \
    galaxy-fds-spark-cleaner-1.0-SNAPSHOT.jar \
    --fds_file_cleaner_base_path "hdfs://${yarn_cluster_name}/home/operator/fdscleaner/aggregator/" \
    --hbase_cluster_name ${hbase_cluster_name} \
    --yarn_cluster_name ${yarn_cluster_name}
