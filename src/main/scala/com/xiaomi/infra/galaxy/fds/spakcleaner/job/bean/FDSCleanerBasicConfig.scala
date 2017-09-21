package com.xiaomi.infra.galaxy.fds.spakcleaner.job.bean

import org.joda.time.DateTime

/**
  * Created by shanjixi on 17/9/17.
  */
case class FDSCleanerBasicConfig(date: DateTime = DateTime.now(),
                                 hbase_cluster_name:String="",
                                 yarn_cluster_name:String="",
                                 fds_file_cleaner_base_path:String = ""
                                )

object FDSCleanerBasicConfigParser{
    implicit val _dateTime = scopt.Read.reads(DateTime.parse)// Don't delete, Implicit Converter From String to DateTime
    val parser = new scopt.OptionParser[FDSCleanerBasicConfig]("FDSCleanerBasicConfig") {
        opt[DateTime]('d', "date") optional() valueName ("<yyyy-MM-dd>") action ((x, c) =>
            c.copy(date = x)) text ("processing date")
        opt[String]('s', "hbase_cluster_name") required() valueName ("<like: c4tst-galaxy-staging>") action ((x, c) =>
            c.copy(hbase_cluster_name = x)) text ("hbase_cluster_name is required")
        opt[String]("fds_file_cleaner_base_path") optional() valueName ("<fds_file_cleaner_base_path>") action ((x, c) =>
            c.copy(fds_file_cleaner_base_path = x)) text ("fds_file_cleaner_base_path")
        opt[String]('s', "yarn_cluster_name") required() valueName ("<like: c4tst-staging>") action ((x, c) =>
            c.copy(yarn_cluster_name = x)) text ("cluster_name is required")
    }
}