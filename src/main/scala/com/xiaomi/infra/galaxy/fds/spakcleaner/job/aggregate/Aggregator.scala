package com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.Date

import com.xiaomi.infra.galaxy.blobstore.hadoop.BlobInfoDao
import com.xiaomi.infra.galaxy.fds.cleaner.dao.hbase.{HBaseAggregatedFileInfoDao, HBaseAggregatedFileMetaDao}
import com.xiaomi.infra.galaxy.fds.dao.hbase.HBaseFDSObjectDao
import com.xiaomi.infra.galaxy.fds.server.FDSConfigKeys
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.{BlobInfoBean, FDSObjectInfoBean, FdsFileStatus}
import com.xiaomi.infra.galaxy.fds.spakcleaner.hbase.FDSObjectHbaseWrapper
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.common.WritableSerDerUtils
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.HBaseContext
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.HBaseRDDFunctions._
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.core.KeyFamilyQualifier
import com.xiaomi.infra.hbase.client.HBaseClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.io.Writable
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Copyright 2017, Xiaomi.
  * All rights reserved.
  * Author: haxiaolin@xiaomi.com
  */
object Aggregator extends Serializable {
    @transient val LOG = LoggerFactory.getLogger(classOf[Aggregator])
    //    case class Config(date: String = "2017-08-10",
    //                      out_put_hdfs_file:String = "hdfs://test/path"
    //                     )
    //    val parser = new scopt.OptionParser[Config]("BasicValidatorJob") {
    //        opt[String]('d', "date") optional() valueName ("<yyyy-MM-dd>") action ((x, c) =>
    //            c.copy(date = x)) text ("processing date")
    //        opt[String]("out_put_hdfs_file") optional() valueName ("<out_put_hdfs_file>") action ((x, c) =>
    //            c.copy(out_put_hdfs_file = x)) text ("out_put_hdfs_file")
    //    }

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("FDS cleaner in scala")
        sparkConf.setIfMissing("spark.master", "local[2]")
        val sc = new SparkContext(sparkConf)
        val aggregator = new Aggregator(sc)
        val ret = aggregator.run()
        if (ret != 0) {
            println("Error In Aggregator")
        }
        System.exit(ret)
    }
}

object WritableSerDerUtils {
    def serialize(writable: Writable): Array[Byte] = {
        val out = new ByteArrayOutputStream()
        val dataOut = new DataOutputStream(out)
        writable.write(dataOut)
        dataOut.close()
        out.toByteArray
    }

    def deserialize(bytes: Array[Byte], writable: Writable): Writable = {
        val in = new ByteArrayInputStream(bytes)
        val dataIn = new DataInputStream(in)
        writable.readFields(dataIn)
        dataIn.close()
        writable
    }
}

class Aggregator(@transient sc: SparkContext) extends Serializable {
    import Aggregator.LOG
    val objectTable = "hbase://c4tst-galaxy-staging/c4tst_galaxy_staging_fds_object_table"
    val fileTable = "hbase://c4tst-galaxy-staging/c4tst_galaxy_staging_galaxy_blobstore_hadoop_fileinfo"
    val blobTable = "hbase://c4tst-galaxy-staging/c4tst_galaxy_staging_galaxy_blobstore_hadoop_blobinfo"
    @transient
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    def run(): Int = {
        val fileIdWithObjects = loadDataFromHBase(sc)
        println(s"Total FDS FileInfo Size:${fileIdWithObjects.count()}")
        val hbaseMeta = new FileStatusCompJob(sc).doComp(fileIdWithObjects)
        val file_table_rdd = hbaseMeta.map(_._1)
        val meta_table_rdd = hbaseMeta.map(_._2)
        //saveFileBackToHbase(file_table_rdd)
        //saveMetaBackToHbase(meta_table_rdd)
        0
    }

    def loadDataFromHBase(@transient sc: SparkContext): RDD[(Long, FDSObjectInfoBean)] = {
        val scan = new Scan()
        val conf = HBaseConfiguration.create(sc.hadoopConfiguration)
        val confBytes = sc.broadcast(WritableSerDerUtils.serialize(conf))

        val b_getBlobInfo_success_counter = sc.accumulator(0L)
        val b_getBlobInfo_fail_counter = sc.accumulator(0L)
        conf.setLong("job.start.timestamp", new Date().getTime)
        scan.addFamily(HBaseFDSObjectDao.BASIC_INFO_COLUMN_FAMILY)
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
                WritableSerDerUtils.deserialize(confBytes.value, conf)
                conf.set("hbase.cluster.name", "c4tst-galaxy-staging");
                conf.set("galaxy.hbase.table.prefix", "c4tst_galaxy_staging_");
                val client = new HBaseClient(conf)
                conf.set(TableInputFormat.INPUT_TABLE, blobTable)
                val blobInfoDao = new BlobInfoDao(client)
                x.map { case (objectKey, uri, size) => {
                    val blobInfo = blobInfoDao.getBlobInfo(uri)
                    if(blobInfo==null){
                        LOG.info("INVALID URL"+uri)
                        b_getBlobInfo_fail_counter.add(1L)
                        None
                    }else{
                        b_getBlobInfo_success_counter.add(1L)
                        val blobInfoBean = BlobInfoBean(blobInfo.getFileId, blobInfo.getBlobId, blobInfo.getStart, blobInfo.getLen)
                        Some(blobInfo.getFileId -> FDSObjectInfoBean(objectKey, size, blobInfoBean))
                    }
                }
                }
            })
            .filter(_.isDefined)
            .map(_.get)
            .persist(StorageLevel.MEMORY_AND_DISK)

            LOG.info("Success GetBlobinfo Count:" + b_getBlobInfo_success_counter.value)
            LOG.info("Fail    GetBlobinfo Count:" + b_getBlobInfo_fail_counter.value)
        rdd2
    }


    def saveFileBackToHbase(fileRDD: RDD[FdsFileStatus]): Unit = {
        val hbaseContext = new HBaseContext(sc, HBaseConfiguration.create())
        fileRDD.loadByRPC(
            hbaseContext,
            fileTable,
            (fdsFileStatus: FdsFileStatus) => {
                val rowKey = Bytes.toBytes(fdsFileStatus.file_id)
                val family = HBaseAggregatedFileInfoDao.BASIC_INFO_COLUMN_FAMILY
                val map = Map[Array[Byte], Array[Byte]](
                    HBaseAggregatedFileInfoDao.SIZE_QUALIFIER -> Bytes.toBytes(fdsFileStatus.remainSize),
                    HBaseAggregatedFileInfoDao.EMPTY_PERCENT_QUALIFIER -> Bytes.toBytes(fdsFileStatus.emptyPercent),
                    HBaseAggregatedFileInfoDao.PATH_QUALIFIER -> Bytes.toBytes(fdsFileStatus.path),
                    HBaseAggregatedFileInfoDao.DELETED_QUALIFIER -> Bytes.toBytes(fdsFileStatus.deleted)
                )
                map.map { case (qualifier, values) => {
                    (new KeyFamilyQualifier(rowKey, family, qualifier), values)
                }
                }
            }
        )
    }

    def saveMetaBackToHbase(metaRDD: RDD[List[FDSObjectHbaseWrapper]]): Unit = {
        val hbaseContext = new HBaseContext(sc, HBaseConfiguration.create())
        val metaTable = sc.getConf.get(FDSConfigKeys.GALAXY_FDS_CLEANER_AGGREGATOR_META_TABLE_KEY)
        metaRDD.loadByRPC(
            hbaseContext,
            metaTable,
            (metaList: List[FDSObjectHbaseWrapper]) => {
                val family = HBaseAggregatedFileMetaDao.BASIC_INFO_COLUMN_FAMILY
                metaList.flatMap(fdsObjectItem => {
                    val metaRowKey = fdsObjectItem.meatRowKey
                    val map = Map[Array[Byte], Array[Byte]](
                        HBaseAggregatedFileMetaDao.OBJECT_KEY_QUALIFIER -> Bytes.toBytes(fdsObjectItem.objectKey),
                        HBaseAggregatedFileMetaDao.START_QUALIFIER -> Bytes.toBytes(fdsObjectItem.start),
                        HBaseAggregatedFileMetaDao.LENGTH_QUALIFIER -> Bytes.toBytes(fdsObjectItem.length)
                    )
                    map.map { case (qualifier, values) => {
                        (new KeyFamilyQualifier(metaRowKey, family, qualifier), values)
                    }
                    }.toIterator
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
