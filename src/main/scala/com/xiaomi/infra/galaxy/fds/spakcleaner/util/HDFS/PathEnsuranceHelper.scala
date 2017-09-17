package com.xiaomi.infra.galaxy.fds.spakcleaner.util.HDFS

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.Logger

/**
  * Created by shanjixi on 17/9/17.
  */
object PathEnsurenceHelper {
    def EnsureOutputFolder(_outputFile: String,LOG:Logger): Boolean = {
        val HDFS = FileSystem.get(new Configuration())
        try {
            HDFS.delete(new Path(_outputFile), true)
            LOG.info("EnsureOutputFolder Successfully"+_outputFile)
            return true;
        } catch {
            case e: IOException => {
                LOG.error("HDFS error",e)
            }
        }
        return false
    }
}
