package com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.core.KeyFamilyQualifier
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.HBaseConfiguration
/**
  * Created by haxiaolin on 17/8/20.
  */
object HBaseRDDFunctions
{
    implicit class GenericHBaseRDDFunctions[T](val rdd: RDD[T]) {
        private val LOG: Log = LogFactory.getLog("HBaseRDDFunctions.GenericHBaseRDDFunctions")

        def loadByRPC(
                     hBaseContext: HBaseContext,
                     tableName: String,
                     flatMap: (T) => Iterable[(KeyFamilyQualifier, Array[Byte])]
                     ): Unit = {
            LOG.debug("Opening HTable \"" + tableName + "\" for writing")
            hBaseContext._doRpcLoad(rdd,tableName,flatMap)
        }
        def loadByBulk(
                      hBaseContext: HBaseContext,
                      tableName:String,
                      flatMap: (T) => Iterator[(KeyFamilyQualifier, Array[Byte])]
                      ):Unit={
            /*Pending As HBase Locator Is Not Available In Hbase 0.94. Nees To find the start Keys For Ragions

             */
        }
    }
}
