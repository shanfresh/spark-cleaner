package com.xiaomi.infra.galaxy.fds.spakcleaner.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by haxiaolin on 17/9/13.
  */
trait SparkContextProvider {
    def sc: SparkContext

    def appID: String = this.getClass.getName + math.floor(math.random * 10E4).toLong.toString

    def conf = {
        new SparkConf().
                setMaster("local[*]").
                setAppName("test").
                set("spark.ui.enabled", "false").
                set("spark.app.id", appID)
    }


    /*
     * Setup work to be called when creating a new SparkContext. Default implementation currently
     * sets a checkpoint directory.
     * This should be called by the context provider automatically.
     */
    def setup(sc: SparkContext): Unit = {
        sc.setCheckpointDir(Utils.createTempDir().toPath().toString)
    }
}
