package com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact

import java.io.IOException

import com.xiaomi.infra.galaxy.blobstore.hadoop.{BlobInfoDao, FileInfoDao, FileManager, HadoopBlobClient}
import com.xiaomi.infra.galaxy.fds.cleaner.CompactionTableSplit
import com.xiaomi.infra.galaxy.fds.cleaner.auditor.{AuditorType, BasicAuditor}
import com.xiaomi.infra.galaxy.fds.dao.hbase.HBaseFDSObjectDao
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.FdsFileStatus
import com.xiaomi.infra.hbase.client.{HBaseClient, HException}
import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * Created by shanjixi on 17/8/21.
  */
class FDSCompactJob(@transient sparkContext: SparkContext) extends Serializable{
    private val LOG = LogFactory.getLog(classOf[FDSCompactJob])
    private var auditor: BasicAuditor = _
    private val conf = sparkContext.hadoopConfiguration
    private var client:HBaseClient = _
    private var fileInfoDao:FileInfoDao = _
    private var blobInfoDao:BlobInfoDao = _
    private var objectDao:HBaseFDSObjectDao = _
    private var blobClient:HadoopBlobClient = _
    private val maxBytes = conf.getLong(FileManager.HADOOP_BLOBSTORE_MAX_FILE_SIZE_BYTES, FileManager.DEFAULT_HADOOP_BLOBSTORE_MAX_FILE_SIZE_BYTES)

    def fdsDAOInit():Unit={
        try{
            client = new HBaseClient(conf)
        }catch {
            case e: HException => {
                LOG.error("Create HBase client failed.")
                throw new IOException("Create HBase client failed.", e)
            }
        }
        fileInfoDao = new FileInfoDao(client)
        blobInfoDao = new BlobInfoDao(client)
        objectDao = new HBaseFDSObjectDao(client)
        try {
            blobClient = new HadoopBlobClient(conf)
        }catch {
            case e: HException => {
                LOG.error("Cannot create blob client")
                throw new IOException(e)
            }
        }
    }
    def run():Unit={
        val inputRDD = readInputFromHDFS()
        val splitsListRDD = inputRDD.mapPartitions(it=>{
            val manager = it.aggregate(SplitManager())(seqOp,comOp)
            manager.splits.toIterator
        }).map(split=>{
            doSomeThingOnSplit(split) // We need to Do Something for this Split
        })

    }

    def readInputFromHDFS():RDD[FdsFileStatus]={
        //Todo: we need to load file from HDFS. File Type:Parquent
        val input = sparkContext.parallelize(Seq[FdsFileStatus]())
        input
    }


    case class Split(fileStatus: List[FdsFileStatus]=List.empty){
        var size = 0L
        def addFile(fdsFileStatus: FdsFileStatus): Unit ={
            this.size += fdsFileStatus.remainSize
        }
        def getSize():Long={
            size
        }
    }
    case class SplitManager(var splits:List[Split]=List.empty,var currentSplit:Split=Split()){
        def addFile(fdsFileStatus: FdsFileStatus):Unit={
            currentSplit.addFile(fdsFileStatus)
            if (currentSplit.getSize >= maxBytes) {
                splits = splits :+ currentSplit
                currentSplit = new Split()
            }
        }
        def merge(other: SplitManager):SplitManager={
            splits = splits ++ other.splits
            other.currentSplit.fileStatus.foreach(file=>{
                addFile(file)
            })
            this
        }
    }
    def seqOp(manager:SplitManager,fdsFileStatus: FdsFileStatus):SplitManager={
        manager.addFile(fdsFileStatus)
        manager
    }
    def comOp(manager: SplitManager,otherManager:SplitManager):SplitManager={
        manager.merge(otherManager)
    }


    def doSomeThingOnSplit(split:Split):String={

        "恭喜发财"
    }

    def initAuditorLogger():Unit={
        if(auditor == null)
            auditor = new BasicAuditor(AuditorType.COMPACT_AUDITOR, sparkContext.hadoopConfiguration , "Retry-1");
    }
}
