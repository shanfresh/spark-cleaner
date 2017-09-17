package com.xiaomi.infra.galaxy.fds.spakcleaner.job.aggregate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.Date

import com.xiaomi.infra.galaxy.blobstore.hadoop.BlobInfoDao
import com.xiaomi.infra.galaxy.fds.dao.hbase.HBaseFDSObjectDao
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.{BlobInfoBean, FDSObjectInfoBean, FdsFileStatus}
import com.xiaomi.infra.galaxy.fds.spakcleaner.hbase.FDSObjectHDFSWrapper
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.HDFSPathFinder
import com.xiaomi.infra.galaxy.fds.spakcleaner.job.bean.{FDSCleanerBasicConfig, FDSCleanerBasicConfigParser}
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.HDFS.PathEnsurenceHelper
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
    val date_time_formatter = "yyyy-MM-dd"
    @transient val LOG = LoggerFactory.getLogger(classOf[Aggregator])

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("FDS cleaner in scala")
        sparkConf.setIfMissing("spark.master", "local[2]")
        val sc = new SparkContext(sparkConf)
        FDSCleanerBasicConfigParser.parser.parse(args,new FDSCleanerBasicConfig()) match{
            case Some(config) => {
                val aggregator = new Aggregator(sc,config)
                val ret = aggregator.run()
                System.exit(ret)
            }
            case _=>{
                LOG.error("Unrecognize Input Main Mehod Arguments:"+args.mkString(" "))
                System.exit(-1)
            }
        }
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

class Aggregator(@transient sc: SparkContext,config:FDSCleanerBasicConfig) extends Serializable {
    import Aggregator._
    val objectTable = s"hbase://${config.cluster_name}/${config.cluster_name}_fds_object_table"
    //val fileTable = s"hbase://${config.cluster_name}/${config.cluster_name}_galaxy_blobstore_hadoop_fileinfo"
    val blobTable = s"hbase://${config.cluster_name}/${config.cluster_name}_galaxy_blobstore_hadoop_blobinfo"
    @transient
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val date_time_str = config.date.toString(date_time_formatter)

    def run(): Int = {
        val fileIdWithObjects = loadDataFromHBase(sc)
        println(s"Total FDS FileInfo Size:${fileIdWithObjects.count()}")
        val hbaseMeta = new FileStatusCompJob(sc).doComp(fileIdWithObjects)
        val file_table_rdd = hbaseMeta.map(_._1)
        val meta_table_rdd = hbaseMeta.map(_._2)

        val fds_file_status_save_flag = saveFileBackToHDFS(file_table_rdd)
        val fds_file_meta_save_flag = saveMetaBackToHDFS(meta_table_rdd)
        if(fds_file_status_save_flag && fds_file_meta_save_flag)
            return 0
        else
            return -1
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

        val rdd2 = hbaseRDD.flatMap(data => {
            val objectKey = Bytes toString data._1.get()
            val uri = Bytes.toString(data._2.getValue(
                HBaseFDSObjectDao.BASIC_INFO_COLUMN_FAMILY,
                HBaseFDSObjectDao.URI_QUALIFIER))
            val size = Bytes.toLong(data._2.getValue(
                HBaseFDSObjectDao.BASIC_INFO_COLUMN_FAMILY,
                HBaseFDSObjectDao.SIZE_QUALIFIER))
            val urlList = uri.split(",")
            urlList.map(smallUri=>
                (objectKey, smallUri,size)
            )
        })
            .filter(_._2 != null).filter(_._2 != "").filter(_._3 != 0)
            .mapPartitions(x => {
                val conf = HBaseConfiguration.create()
                WritableSerDerUtils.deserialize(confBytes.value, conf)
                conf.set("hbase.cluster.name", config.cluster_name)
                conf.set("galaxy.hbase.table.prefix", s"${config.cluster_name}_")
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


    def saveFileBackToHDFS(fileRDD: RDD[FdsFileStatus]): Boolean = {
        import sqlContext.implicits._
        val path = HDFSPathFinder.getAggergatorFileStatusByDate(config.fds_file_cleaner_base_path,date_time_str)
        val path_is_ready = PathEnsurenceHelper.EnsureOutputFolder(path,LOG)
        if(!path_is_ready)
            return false;
        fileRDD
            .toDF()
            .write
            .parquet(path)
        LOG.info(s"Save File Status Info HDFS successfully,path:${path}")
        return true;
    }

    def saveMetaBackToHDFS(metaRDD: RDD[List[FDSObjectHDFSWrapper]]): Boolean = {
        import sqlContext.implicits._
        val path = HDFSPathFinder.getAggergatorFileMetaByDate(config.fds_file_cleaner_base_path,date_time_str)
        val path_is_ready = PathEnsurenceHelper.EnsureOutputFolder(path,LOG)
        if(!path_is_ready)
            return false;
        metaRDD
            .flatMap(list=>list)
            .toDF
            .write
            .parquet(path)
        LOG.info(s"Save File Meta Info HDFS successfully,path:${path}")
        return true
    }

    def convertScanToString(scan: Scan): String = {
        val out = new ByteArrayOutputStream()
        val dos = new DataOutputStream(out)
        scan.write(dos)
        return Base64.encodeBytes(out.toByteArray())
    }
}
