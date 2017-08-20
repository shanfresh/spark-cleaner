package com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact

import java.io.IOException

import com.xiaomi.infra.galaxy.blobstore.hadoop.{BlobInfoDao, FileInfoDao, HadoopBlobClient}
import com.xiaomi.infra.galaxy.fds.cleaner.auditor.{AuditorType, BasicAuditor}
import com.xiaomi.infra.galaxy.fds.dao.hbase.HBaseFDSObjectDao
import com.xiaomi.infra.hbase.client.HException
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.ipc.HBaseClient
import org.apache.spark.{SparkContext, SparkEnv}

/**
  * Created by shanjixi on 17/8/21.
  */
class FDSCompactJob(@transient sparkContext: SparkContext) extends Serializable{
    private val LOG = LogFactory.getLog(classOf[FDSCompactJob])
    private val client = _
    private var auditor: BasicAuditor = _
    private val conf = sparkContext.hadoopConfiguration

    def fdsDAOInit():Unit={
        try
            client = new HBaseClient(conf)
        catch {
            case e: HException => {
                LOG.error("Create HBase client failed.")
                throw new IOException("Create HBase client failed.", e)
            }
        }
        fileInfoDao = new FileInfoDao(client)
        blobInfoDao = new BlobInfoDao(client)
        objectDao = new HBaseFDSObjectDao(client)
        try
            blobClient = new HadoopBlobClient(conf)

        catch {
            case e: HException => {
                LOG.error("Cannot create blob client")
                throw new IOException(e)
            }
        }
    }

    def initAuditorLogger():Unit={
        if(auditor == null)
            auditor = new BasicAuditor(AuditorType.COMPACT_AUDITOR, sparkContext.hadoopConfiguration , "Retry-1");
    }
}
