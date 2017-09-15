package com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase

import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.core.KeyFamilyQualifier
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD

/**
  * Created by haxiaolin on 17/8/20.
  */
class HBaseSparkWriter {
    def _getConf(): Configuration = {
        val conf = HBaseConfiguration.create()
        conf
    }

    def doBulk[T](rdd: RDD[T],
                  tableName: String,
                  flatMap: (T) => Iterator[(KeyFamilyQualifier, Array[Byte])],
                  stagingDir: String): Unit = {

    }

    def doRPCLoad[T](rdd: RDD[T],
                     tableName: String,
                     flatMap: (T) => Iterator[(KeyFamilyQualifier, Array[Byte])]): Unit = {


    }
}
