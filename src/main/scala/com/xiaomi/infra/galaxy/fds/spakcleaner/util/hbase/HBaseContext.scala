package com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase
import com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.core.KeyFamilyQualifier
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SerializableWritable, SparkContext}
/**
  * Created by haxiaolin on 17/8/20.
  */
class HBaseContext(@transient sc: SparkContext,
                   @transient val config: Configuration) extends Serializable{
    val broadcastedConf = sc.broadcast(new SerializableWritable(config))
    val LOG = LogFactory.getLog(classOf[HBaseContext])
    @transient var tmpHdfsConfiguration:Configuration = config
    def _doRpcLoad[T](rdd:RDD[T],
                      tableName:String,
                      flatMap: (T) => Iterable[(KeyFamilyQualifier, Array[Byte])]
                  ): Unit ={
        rdd.foreachPartition(it=>{
            val config = getConf(broadcastedConf)
            val table: HTable = new HTable(config, Bytes.toBytes(tableName))
            table.setAutoFlush(false)
            it.flatMap(flatMap).foreach{
                case (keyFamilyQualifier:KeyFamilyQualifier,values:Array[Byte])=>{
                    val key = keyFamilyQualifier.toString
                    val value = values.toString
                    println(key+"===>"+value)
                }
            }
        })
    }
    private def getConf(configBroadcast: Broadcast[SerializableWritable[Configuration]]): Configuration = {
        if (tmpHdfsConfiguration == null) {
            try {
                tmpHdfsConfiguration = configBroadcast.value.value
            } catch {
                case ex: Exception => LOG.error("Unable to getConfig from broadcast", ex)
            }
        }
        tmpHdfsConfiguration
    }
}
