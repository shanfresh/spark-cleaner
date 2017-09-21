package com.xiaomi.infra.galaxy.fds.spakcleaner.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkContext}

/**
  * Created by shanjixi on 17/9/21.
  */
object HabaseConfigurationManager {
    def getHBaseConfiguration(sc:SparkContext):Configuration={
        val conf = HBaseConfiguration.create(sc.hadoopConfiguration)
        conf
    }
}
