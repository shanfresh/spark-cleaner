package com.xiaomi.infra.galaxy.fds.spakcleaner.job

/**
  * Created by shanjixi on 17/9/17.
  */
object HDFSPathFinder {
    def getAggergatorFileStatusByDate(base_path:String,date_time:String):String={
        s"${base_path}/aggregator/${date_time}/file_status"
    }
    def getAggergatorFileMetaByDate(base_path:String,date_time:String):String={
        s"${base_path}/aggregator/${date_time}/file_meta"
    }
}
