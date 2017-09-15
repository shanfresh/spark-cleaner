package com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate

import com.xiaomi.infra.galaxy.blobstore.utils.CodecUtil
import com.xiaomi.infra.galaxy.fds.cleaner.dao.hbase.HBaseAggregatedFileMetaDao
import com.xiaomi.infra.galaxy.fds.cleaner.utils.CleanerUtils
import com.xiaomi.infra.galaxy.fds.server.FDSConfigKeys
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.{FDSObjectInfoBean, FdsFileStatus}
import com.xiaomi.infra.galaxy.fds.spakcleaner.hbase.FDSObjectHbaseWrapper
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.Path

/**
  * Created by haxiaolin on 17/8/10.
  */
class FileStatusCompJob(@transient sc: SparkContext) extends Serializable {
    val archiveBucketname= FDSConfigKeys.GALAXY_FDS_CLEANER_ARCHIVE_BUCKET_NAME_DEFAULT
    private def isAllAarchive(list:Vector[FDSObjectInfoBean]):Boolean={
        val has_plant_file = list.find(bean=> !bean.objectKey.startsWith("archiveBucketName/")).isDefined
        !has_plant_file
    }
    private def isArchive(fDSObjectInfoBean: FDSObjectInfoBean):Boolean ={
        fDSObjectInfoBean.objectKey.startsWith(archiveBucketname + "/")
    }
    private def getFileStatus(file_id:Long,list:Vector[FDSObjectInfoBean]):(FdsFileStatus,List[FDSObjectHbaseWrapper])={
        import util.control.Breaks._
        var allBeanArchived = true
        var remainSize:Long = 0L
        for(info <-list){
            breakable{
                if (info.objectKey.startsWith(archiveBucketname + "/"))
                    break()
                allBeanArchived = false
                remainSize += info.blobInfo.length
            }
        }
        val path: Path = CleanerUtils.formatFilePath(file_id, sc.hadoopConfiguration)

        val fileStatus = if(allBeanArchived){
            FdsFileStatus(file_id,-1,Long.MaxValue,path.toString,false)
        }else{
            val fs = FileSystem.get(sc.hadoopConfiguration)
            val path = CleanerUtils.formatFilePath(file_id, sc.hadoopConfiguration)
            val status = fs.getFileStatus(path)
            val totalSize = status.getLen
            val emptyPercent = if (totalSize == 0) -1 else ((totalSize - remainSize) * 100 / totalSize).toInt
            FdsFileStatus(file_id,emptyPercent,remainSize,path.toString,false)
        }
        val hbaseObjectList = list
                .filter(bean=> !isArchive(bean))
                .map(bean=>{
                    val metaRowKey: Array[Byte] = Bytes.toBytes(file_id.toString + HBaseAggregatedFileMetaDao.KEY_DELIMITER + bean.blobInfo.blobId)
                    FDSObjectHbaseWrapper(metaRowKey,bean.objectKey,bean.blobInfo.start,bean.blobInfo.length)
                }).toList
        (fileStatus,hbaseObjectList)
    }

    def doComp(source:RDD[(Long,FDSObjectInfoBean)]):RDD[(FdsFileStatus,List[FDSObjectHbaseWrapper])] ={
        val file_status_rdd = source
                .groupByKey()
                .map { case (key, objectList) => {
                    val (fileStatus, hbaseList) = getFileStatus(key, objectList.toVector)
                    (fileStatus, hbaseList)
                }}
                .persist(StorageLevel.MEMORY_AND_DISK)
        file_status_rdd
    }

}
