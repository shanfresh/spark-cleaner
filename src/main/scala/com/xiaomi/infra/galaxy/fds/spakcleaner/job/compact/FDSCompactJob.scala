package com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact

import java.io.IOException

import com.xiaomi.infra.galaxy.blobstore.hadoop.{BlobInfoDao, FileInfoDao, FileManager, HadoopBlobClient}
import com.xiaomi.infra.galaxy.fds.cleaner.auditor.{AuditorType, BasicAuditor}
import com.xiaomi.infra.galaxy.fds.dao.hbase.HBaseFDSObjectDao
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.FdsFileStatus
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact.split.{Split, SplitManager}
import com.xiaomi.infra.hbase.client.HException
import org.apache.commons.logging.LogFactory
import com.xiaomi.infra.hbase.client.HBaseClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * Created by haxiaolin on 17/8/21.
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
        val splitsListRDD =fdsFileStatusToSplit(inputRDD)

    }

    def readInputFromHDFS():RDD[FdsFileStatus]={
        //Todo: we need to load file from HDFS. File Type:Parquent
        val input = sparkContext.parallelize(Seq[FdsFileStatus]())
        input
    }

    def fdsFileStatusToSplit(status:RDD[FdsFileStatus]):RDD[Split]={
        def seqOp(manager:SplitManager,fdsFileStatus: FdsFileStatus):SplitManager={
            manager.addFile(fdsFileStatus)
            manager
        }
        def comOp(manager: SplitManager,otherManager:SplitManager):SplitManager={
            manager.merge(otherManager)
        }

        status.mapPartitions(it=>{
            val manager = it.aggregate(SplitManager())(seqOp,comOp)
            manager.getAllSplitsIterator()
        })
    }


    def doSomeThingOnSplit(split:Split):String={
        "恭喜发财"
    }

    def initAuditorLogger():Unit={
        if(auditor == null)
            auditor = new BasicAuditor(AuditorType.COMPACT_AUDITOR, sparkContext.hadoopConfiguration , "Retry-1");
    }
}
