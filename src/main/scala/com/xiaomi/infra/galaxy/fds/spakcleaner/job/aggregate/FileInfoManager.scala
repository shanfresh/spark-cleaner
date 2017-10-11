package com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate

import com.xiaomi.infra.galaxy.blobstore.hadoop.{BlobInfoDao, FileInfoDao}
import com.xiaomi.infra.galaxy.fds.cleaner.dao.hbase.HBaseAggregatedFileMetaDao
import com.xiaomi.infra.galaxy.fds.cleaner.utils.CleanerUtils
import com.xiaomi.infra.galaxy.fds.server.FDSConfigKeys
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.{FDSObjectInfoBean, FdsFileStatus}
import com.xiaomi.infra.galaxy.fds.spakcleaner.hbase.FDSObjectHDFSWrapper
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.HabaseConfigurationManager
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.bean.FDSCleanerBasicConfig
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.TableHelper
import com.xiaomi.infra.hbase.client.HBaseClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
  * Copyright 2017, Xiaomi.
  * All rights reserved.
  * Author: haxiaolin@xiaomi.com
  */
class FileInfoManager(@transient sc: SparkContext, config: FDSCleanerBasicConfig) extends Serializable {
    val LOG = LoggerFactory.getLogger(classOf[FileInfoManager])
    val archiveBucketName = FDSConfigKeys.GALAXY_FDS_CLEANER_ARCHIVE_BUCKET_NAME_DEFAULT
    val fileTable: String = TableHelper.getWholeTableName(config.hbase_cluster_name, "galaxy_blobstore_hadoop_fileinfo")
    val b_hbase_configurations = serializeBroudcastConfigurationBytes()
    def isArchive(fDSObjectInfoBean: FDSObjectInfoBean): Boolean = {
        fDSObjectInfoBean.objectKey.startsWith(archiveBucketName + "/")
    }
    def serializeBroudcastConfigurationBytes():Broadcast[Array[Byte]]={
        val conf = HabaseConfigurationManager.getHBaseConfiguration(sc)
        val confBytes = sc.broadcast(WritableSerDerUtils.serialize(conf))
        confBytes
    }
    def deserializeHbaseConfiguration():Configuration={
        val conf = HBaseConfiguration.create()
        WritableSerDerUtils.deserialize(b_hbase_configurations.value, conf)
        conf
    }

    private def getFileStatus(file_id: Long, list: Vector[FDSObjectInfoBean],fileInfoDao: FileInfoDao,fileSystem: FileSystem): Option[(FdsFileStatus, List[FDSObjectHDFSWrapper])] = {
        import util.control.Breaks._
        var allBeanArchived = true
        var remainSize: Long = 0L
        for (info <- list) {
            breakable {
                if (info.objectKey.startsWith(archiveBucketName + "/"))
                    break()
                allBeanArchived = false
                remainSize += info.blobInfo.length
            }
        }
        val fileInfo = fileInfoDao.getFile(file_id)
        if(fileInfo == null || fileInfo.getPath == null)
            return None
        val path = fileInfo.getPath
        val fileStatus = if (allBeanArchived) {
            FdsFileStatus(file_id, -1, Long.MaxValue, path.toString, false)
        } else {
            val status = fileSystem.getFileStatus(path)
            val totalSize = status.getLen
            val emptyPercent = if (totalSize == 0) -1 else ((totalSize - remainSize) * 100 / totalSize).toInt
            FdsFileStatus(file_id, emptyPercent, remainSize, path.toString, false)
        }
        val hbaseObjectList = list
            .filter(bean => !isArchive(bean))
            .map(bean => {
                val metaRowKey = file_id.toString + HBaseAggregatedFileMetaDao.KEY_DELIMITER + bean.blobInfo.blobId
                FDSObjectHDFSWrapper(metaRowKey, bean.objectKey, bean.blobInfo.start, bean.blobInfo.length)
            }).toList
        Some(fileStatus, hbaseObjectList)
    }

    def doComp(source: RDD[(Long, FDSObjectInfoBean)]): RDD[(FdsFileStatus, List[FDSObjectHDFSWrapper])] = {
        require(sc!=null, "SPARK CONTEXT SHOULD NOT BE NULL")
        val b_hadoop_configuration_ser= sc.broadcast(WritableSerDerUtils.serialize(sc.hadoopConfiguration))
        val file_status_rdd = source
            .groupByKey()
            .mapPartitions(it=>{
                val hbase_cluster_configuration = deserializeHbaseConfiguration()
                val hadoop_cluster_configuration = new Configuration()
                require(hadoop_cluster_configuration!=null, "HADOOP CLUSTER CONTEXT SHOULD NOT BE NULL")

                WritableSerDerUtils.deserialize(b_hadoop_configuration_ser.value,hadoop_cluster_configuration)
                hbase_cluster_configuration.set("hbase.cluster.name", config.hbase_cluster_name)
                hbase_cluster_configuration.set("galaxy.hbase.table.prefix", s"${TableHelper.getTablePrefix(config.hbase_cluster_name)}_")
                hbase_cluster_configuration.set(TableInputFormat.INPUT_TABLE, fileTable)
                val client = new HBaseClient(hbase_cluster_configuration)
                val fs = FileSystem.get(hadoop_cluster_configuration)
                val fileInfoDao = new FileInfoDao(client)
                it.map{
                    case (key, objectList) => {
                        try{
                            val fileStatusOption= getFileStatus(key, objectList.toVector,fileInfoDao,fs)
                            fileStatusOption
                        }catch {
                            case e:Exception=>{
                                LOG.error("Error when getFileStatus,Exception:",e)
                                None
                            }
                        }
                    }
                }.filter(_.isDefined)
                .map(_.get)
            })
        file_status_rdd
    }
}
