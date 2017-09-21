package com.xiaomi.infra.galaxy.fds.spakcleaner.util.HDFS

import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by shanjixi on 17/9/17.
  */
object HDFSPathFinder {
    def getAggergatorFileStatusByDayAndTs(base_path:String,date_day:String,timestamp:Long):String={
        assert(base_path.last == '/')
        s"${base_path}aggregator/${date_day}/${timestamp}/file_status"
    }
    def getAggergatorFileMetaByDayAndTS(base_path:String,date_day:String,timestamp:Long):String={
        assert(base_path.last == '/')
        s"${base_path}aggregator/${date_day}/${timestamp}/file_meta"
    }

    def getAggergatorFileStatusByDate(base_path:String,date:DateTime):String={
        val date_str = date.toString("yyyy-MM-dd")
        getAggergatorFileStatusByDayAndTs(base_path,date_str,date.getMillis)
    }
    def getAggergatorFileMetaByDate(base_path:String,date:DateTime):String={
        val date_str = date.toString("yyyy-MM-dd")
        getAggergatorFileMetaByDayAndTS(base_path,date_str,date.getMillis)
    }

    def getAggretatorFileStatusByBaseFolerAndSubFoler(baseFolder:String,subFolder:String):String={
        assert(baseFolder.last == '/')
        s"${subFolder}${subFolder}"
    }
    def getAggregatorTimestampFromHDFSFile(aggregator_full_path:String):Long={
        val input_arrays = aggregator_full_path.split("/")
        val longOption = Try(input_arrays.reverse.apply(1).toLong)
        assert(longOption.isSuccess)
        if(longOption.isFailure){
            LoggerFactory.getLogger("HDFSPathFinder").error(s"File Input Path is Invalid:${aggregator_full_path}")
        }else{
            val date_time = new DateTime(longOption.get)
            LoggerFactory.getLogger("HDFSPathFinder").info(s"Got Aggregator Start Timestamp:${date_time.toString}, PATH:${aggregator_full_path}")
        }
        longOption.get
    }
}
