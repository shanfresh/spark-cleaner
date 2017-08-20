package com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util.Date

import com.xiaomi.infra.galaxy.blobstore.hadoop.BlobInfoDao
import com.xiaomi.infra.galaxy.blobstore.utils.CodecUtil
import com.xiaomi.infra.galaxy.fds.cleaner.dao.hbase.{HBaseAggregatedFileInfoDao, HBaseAggregatedFileMetaDao}
import com.xiaomi.infra.galaxy.fds.dao.hbase.HBaseFDSObjectDao
import com.xiaomi.infra.galaxy.fds.model.FDSObjectMetadata
import com.xiaomi.infra.galaxy.fds.server.FDSConfigKeys
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.{BlobInfoBean, FDSObjectInfoBean, FdsFileStatus}
import com.xiaomi.infra.galaxy.fds.spakcleaner.hbase.FDSObjectHbaseWrapper
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate.Aggregator.Config
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.HBaseContext
import com.xiaomi.infra.hbase.client.HBaseClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.slf4j.LoggerFactory
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.HBaseRDDFunctions._
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.core.KeyFamilyQualifier


/**
  * Copyright 2017, Xiaomi.
  * All rights reserved.
  * Author: haxiaolin@xiaomi.com
  */
object Aggregator extends Serializable {
    @transient val LOG = LoggerFactory.getLogger(classOf[Aggregator])
    case class Config(date: String = "2017-08-10",
                      out_put_hdfs_file:String = "hdfs://test/path"
                     )
    val parser = new scopt.OptionParser[Config]("BasicValidatorJob") {
        opt[String]('d', "date") optional() valueName ("<yyyy-MM-dd>") action ((x, c) =>
            c.copy(date = x)) text ("processing date")
        opt[String]("out_put_hdfs_file") optional() valueName ("<out_put_hdfs_file>") action ((x, c) =>
            c.copy(out_put_hdfs_file = x)) text ("out_put_hdfs_file")
    }

    def main(args: Array[String]):Unit = {
        parser.parse(args, Config()) match {
            case Some(config) =>{
                val sparkConf = new SparkConf().setAppName("Spark Codelab: WordCount in scala")
                sparkConf.setIfMissing("spark.master", "local")
                val sc = new SparkContext(sparkConf)
                val aggregator = new Aggregator(sc,config)
                val ret = aggregator.run()
                if(ret !=0){
                    println("Error In Aggregator")
                }
                System.exit(ret)
            }
            case _ => sys.exit(-1)
        }
    }
}

class Aggregator(@transient sc: SparkContext,config:Config) extends Serializable {
    val objectTable = "hbase://c4tst-emq-staging/objectTable"
    val fileTable = "hbase://c4tst-emq-staging/fileTable"
    val blobTable = "hbase://c4tst-emq-staging/blobTable"
    @transient
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    def run():Int={
        val fileIdWithObjects = loadDataFromHbase()
        println(s"Total FDS FileInfo Size:${fileIdWithObjects.count()}")
        val hbaseMeta = new FileStatusCompJob(sc).doComp(fileIdWithObjects)
        val file_table_rdd = hbaseMeta.map(_._1)
        val meta_table_rdd = hbaseMeta.map(_._2)
        saveFileBackToHbase(file_table_rdd)
        saveMetaBackToHbase(meta_table_rdd)
        0
    }
    def loadDataFromHbase(): RDD[(Long, FDSObjectInfoBean)] ={
        val scan = new Scan()
        val conf = HBaseConfiguration.create()
        conf.setLong("job.start.timestamp", new Date().getTime)
        scan.addFamily("BasicInfo".getBytes())
        conf.set(TableInputFormat.INPUT_TABLE, objectTable)
        conf.set(TableInputFormat.SCAN, convertScanToString(scan))
        val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result])

        val rdd2 = hbaseRDD.map(data => {
            val objectKey = Bytes toString data._1.get()
            val uri = Bytes.toString(data._2.getValue(
                HBaseFDSObjectDao.BASIC_INFO_COLUMN_FAMILY,
                HBaseFDSObjectDao.URI_QUALIFIER))
            val size = Bytes.toLong(data._2.getValue(
                HBaseFDSObjectDao.BASIC_INFO_COLUMN_FAMILY,
                HBaseFDSObjectDao.SIZE_QUALIFIER))
            (objectKey, uri, size)
        })
        .filter(_._2 != null).filter(_._2 != "").filter(_._3 != 0)
        .mapPartitions(x => {
            val conf = HBaseConfiguration.create()
            val client = new HBaseClient(conf)
            val blobInfoDao = new BlobInfoDao(client)
            x.map{case (objectKey,uri,size)=>{
                val blobInfo = blobInfoDao.getBlobInfo(uri)
                val blobInfoBean = BlobInfoBean(blobInfo.getFileId,blobInfo.getBlobId,blobInfo.getStart,blobInfo.getLen)
                blobInfo.getFileId -> FDSObjectInfoBean(objectKey,size,blobInfoBean)
            }}
        })
        .persist(StorageLevel.MEMORY_AND_DISK)
        rdd2
    }
    def saveFileBackToHbase(fileRDD:RDD[FdsFileStatus]):Unit={
        val hbaseContext = new HBaseContext(sc,HBaseConfiguration.create())
        fileRDD.loadByRPC(
            hbaseContext,
            fileTable,
            (fdsFileStatus:FdsFileStatus)=>{
                val rowKey = Bytes.toBytes(fdsFileStatus.file_id)
                val family = HBaseAggregatedFileInfoDao.BASIC_INFO_COLUMN_FAMILY
                val map = Map[Array[Byte],Array[Byte]](
                    HBaseAggregatedFileInfoDao.SIZE_QUALIFIER->Bytes.toBytes(fdsFileStatus.remainSize),
                    HBaseAggregatedFileInfoDao.EMPTY_PERCENT_QUALIFIER->Bytes.toBytes(fdsFileStatus.emptyPercent),
                    HBaseAggregatedFileInfoDao.PATH_QUALIFIER->Bytes.toBytes(fdsFileStatus.path),
                    HBaseAggregatedFileInfoDao.DELETED_QUALIFIER->Bytes.toBytes(fdsFileStatus.deleted)
                )
                map.map{case (qualifier,values)=>{
                    (new KeyFamilyQualifier(rowKey,family, qualifier),values)
                }}
            }
        )
    }
    def saveMetaBackToHbase(metaRDD:RDD[List[FDSObjectHbaseWrapper]]):Unit={
        val hbaseContext = new HBaseContext(sc,HBaseConfiguration.create())
        val metaTable = sc.getConf.get(FDSConfigKeys.GALAXY_FDS_CLEANER_AGGREGATOR_META_TABLE_KEY)
        metaRDD.loadByRPC(
            hbaseContext,
            metaTable,
            (metaList:List[FDSObjectHbaseWrapper])=>{
                val family = HBaseAggregatedFileMetaDao.BASIC_INFO_COLUMN_FAMILY
                metaList.flatMap(fdsObjectItem=>{
                    val metaRowKey = fdsObjectItem.meatRowKey
                    val map = Map[Array[Byte],Array[Byte]](
                        HBaseAggregatedFileMetaDao.OBJECT_KEY_QUALIFIER->Bytes.toBytes(fdsObjectItem.objectKey),
                        HBaseAggregatedFileMetaDao.START_QUALIFIER->Bytes.toBytes(fdsObjectItem.start),
                        HBaseAggregatedFileMetaDao.LENGTH_QUALIFIER->Bytes.toBytes(fdsObjectItem.length)
                    )
                    map.map{case (qualifier,values)=>{
                        (new KeyFamilyQualifier(metaRowKey,family, qualifier),values)
                    }}.toIterator
                })
            }
        )
    }

    def convertScanToString(scan: Scan): String = {
        val out = new ByteArrayOutputStream()
        val dos = new DataOutputStream(out)
        scan.write(dos)
        return Base64.encodeBytes(out.toByteArray())
    }
}