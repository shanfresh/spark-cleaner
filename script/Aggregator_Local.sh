SPARK_BIN="/home/haxiaolin/infra-client/bin"
HBASE_CLUSTER_NAME="c4tst-galaxy-staging"
YARN_CLUSTER_NAME="c4tst-staging"
${SPARK_BIN}/spark-submit
--cluster ${YARN_CLUSTER_NAME}
--class com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate.Aggregator
--master yarn-cluster
--hbase hbase://${HBASE_CLUSTER_NAME}/c4tst_galaxy_staging_fds_object_table,hbase://${HBASE_CLUSTER_NAME}/c4tst_galaxy_staging_galaxy_blobstore_hadoop_blobinfo
--num-executors 3
--driver-memory 4g
--executor-memory 2g
--executor-cores 1
--properties-file local.conf
~/spark-cleaner/galaxy-fds-spark-cleaner-1.0-SNAPSHOT.jar
--hbase_cluster_name ${HBASE_CLUSTER_NAME}
--fds_file_cleaner_base_path "hdfs://${YARN_CLUSTER_NAME}/home/operator/fdscleaner/aggregator/"
--yarn_cluster_name ${YARN_CLUSTER_NAME}