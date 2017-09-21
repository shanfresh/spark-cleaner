package com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate

import com.xiaomi.infra.galaxy.blobstore.hadoop.{BlobInfoDao, FileInfoDao}
import com.xiaomi.infra.galaxy.fds.cleaner.dao.hbase.HBaseAggregatedFileMetaDao
import com.xiaomi.infra.galaxy.fds.cleaner.utils.CleanerUtils
import com.xiaomi.infra.galaxy.fds.server.FDSConfigKeys
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.{FDSObjectInfoBean, FdsFileStatus}
import com.xiaomi.infra.galaxy.fds.spakcleaner.hbase.FDSObjectHDFSWrapper
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.bean.FDSCleanerBasicConfig
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.TableHelper
import com.xiaomi.infra.hbase.client.HBaseClient
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Copyright 2017, Xiaomi.
  * All rights reserved.
  * Author: haxiaolin@xiaomi.com
  */
class FileInfoManager(@transient sc: SparkContext, config: FDSCleanerBasicConfig) extends Serializable {
    val archiveBucketName = FDSConfigKeys.GALAXY_FDS_CLEANER_ARCHIVE_BUCKET_NAME_DEFAULT
    val fileTable: String = TableHelper.getWholeTableName(config.hbase_cluster_name, "galaxy_blobstore_hadoop_fileinfo")

    def isArchive(fDSObjectInfoBean: FDSObjectInfoBean): Boolean = {
        fDSObjectInfoBean.objectKey.startsWith(archiveBucketName + "/")
    }

    def getFileStatus(file_id: Long, list: Vector[FDSObjectInfoBean]): (FdsFileStatus, List[FDSObjectHDFSWrapper]) = {
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
        val conf = HBaseConfiguration.create(sc.hadoopConfiguration)
        conf.set("hbase.cluster.name", config.hbase_cluster_name)
        conf.set("galaxy.hbase.table.prefix", s"${TableHelper.getTablePrefix(config.hbase_cluster_name)}_")
        val client = new HBaseClient(conf)
        conf.set(TableInputFormat.INPUT_TABLE, fileTable)
        val fileInfoDao = new FileInfoDao(client)
        val path: Path = fileInfoDao.getFile(file_id).getPath

        val fileStatus = if (allBeanArchived) {
            FdsFileStatus(file_id, -1, Long.MaxValue, path.toString, false)
        } else {
            val fs = FileSystem.get(sc.hadoopConfiguration)
            val path = CleanerUtils.formatFilePath(file_id, sc.hadoopConfiguration)
            val status = fs.getFileStatus(path)
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
        (fileStatus, hbaseObjectList)
    }

    def doComp(source: RDD[(Long, FDSObjectInfoBean)]): RDD[(FdsFileStatus, List[FDSObjectHDFSWrapper])] = {
        val file_status_rdd = source
            .groupByKey()
            .map { case (key, objectList) => {
                val (fileStatus, fdsObjectList) = getFileStatus(key, objectList.toVector)
                (fileStatus, fdsObjectList)
            }
            }
            .persist(StorageLevel.MEMORY_AND_DISK)
        file_status_rdd
    }

}
