package com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact

import java.io.IOException

import com.xiaomi.infra.galaxy.blobstore.hadoop.{BlobInfoDao, FileInfoDao, FileManager, HadoopBlobClient}
import com.xiaomi.infra.galaxy.fds.cleaner.auditor.{AuditorType, BasicAuditor}
import com.xiaomi.infra.galaxy.fds.dao.hbase.HBaseFDSObjectDao
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.FdsFileStatus
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.HDFSPathFinder
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact.FDSCompactJob.FDSCleanerCompactorConfig
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact.split.{Split, SplitManager}
import com.xiaomi.infra.hbase.client.HException
import com.xiaomi.infra.hbase.client.HBaseClient
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

/**
  * Created by haxiaolin on 17/8/21.
  */
object FDSCompactJob{
    val date_time_formatter = "yyyy-MM-dd"
    @transient val LOG = LoggerFactory.getLogger(classOf[FDSCompactJob])
    case class FDSCleanerCompactorConfig(date: DateTime = DateTime.now(),
                                          fds_file_cleaner_base_path:String = ""
                                         )
    implicit val _dateTime = scopt.Read.reads(DateTime.parse)
    val parser = new scopt.OptionParser[FDSCleanerCompactorConfig]("FDSCompactJob") {
        opt[DateTime]('d', "date") optional() valueName ("<yyyy-MM-dd>") action ((x, c) =>
            c.copy(date = x)) text ("processing date")
        opt[String]("fds_file_cleaner_base_path") required() valueName ("<fds_file_cleaner_base_path>") action ((x, c) =>
            c.copy(fds_file_cleaner_base_path = x)) text ("fds_file_cleaner_base_path")
    }

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("FDS CompactJob in scala")
        val sc = new SparkContext(sparkConf)
        parser.parse(args,new FDSCleanerCompactorConfig()) match{
            case Some(config) => {
                val compactor = new FDSCompactJob(sc,config)
                val ret = compactor.run()
                System.exit(ret)
            }
            case _=>{
                LOG.error("Unrecognize Input Main Mehod Arguments:"+args.mkString(" "))
                System.exit(-1)
            }
        }
    }
}

class FDSCompactJob(@transient sparkContext: SparkContext,config:FDSCleanerCompactorConfig) extends Serializable{
    import FDSCompactJob._
    private val date_time_str = config.date.toString(date_time_formatter)
    private var auditor: BasicAuditor = _
    private val conf = sparkContext.hadoopConfiguration
    private var client:HBaseClient = _
    private var fileInfoDao:FileInfoDao = _
    private var blobInfoDao:BlobInfoDao = _
    private var objectDao:HBaseFDSObjectDao = _
    private var blobClient:HadoopBlobClient = _

    @transient
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    @transient
    val a_input_aggregator = sparkContext.accumulator(0L)

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
    def run():Int={
        val inputRDD = readInputFromHDFS()
        val splitsListRDD = fdsFileStatusToSplit(inputRDD)
        splitsListRDD.persist()
        LOG.info(s"Input Aggregator Rows Size:${a_input_aggregator.value}")
        0
    }

    def readInputFromHDFS():RDD[FdsFileStatus]={
        val input_hdfs_path = HDFSPathFinder.getAggergatorFileStatusByDate(config.fds_file_cleaner_base_path,date_time_str)
        val input =
            sqlContext.read.parquet(input_hdfs_path).map(row=>{
                val file_id = row.getAs[Long]("file_id")
                val emptyPercent = row.getAs[Int]("emptyPercent")
                val remainSize = row.getAs[Long]("remainSize")
                val path = row.getAs[String]("path")
                val deleted = row.getAs[Boolean]("deleted")
                a_input_aggregator.add(1L)
                FdsFileStatus(file_id,emptyPercent,remainSize,path,deleted)
            })
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
