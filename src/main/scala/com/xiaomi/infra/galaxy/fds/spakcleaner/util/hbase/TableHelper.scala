package com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase

/**
  * Copyright 2017, Xiaomi.
  * All rights reserved.
  * Author: haxiaolin@xiaomi.com
  */
object TableHelper {
    def getWholeTableName(hbase_cluster_name:String, table_name_suffix:String):String={
        return s"hbase://${hbase_cluster_name}/${getTablePrefix(hbase_cluster_name)}_${table_name_suffix}"
    }

    def getTablePrefix(hbase_cluster_name: String): String = {
        hbase_cluster_name.replaceAll("-", "_")
    }
}
