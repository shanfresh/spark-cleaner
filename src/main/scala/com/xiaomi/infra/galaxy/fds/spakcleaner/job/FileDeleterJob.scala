package com.xiaomi.infra.galaxy.fds.spakcleaner.job

import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.FdsFileStatus
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  * Created by shanjixi on 17/8/10.
  */
class FileDeleterJob(start_time:DateTime) extends Serializable{
    def deleteFile(file_rdd:RDD[FdsFileStatus]):Boolean={
        true
    }

}
