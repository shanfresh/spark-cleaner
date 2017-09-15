package com.xiaomi.infra.galaxy.fds.spakcleaner.basic

import org.apache.spark.{SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
  * Created by haxiaolin on 17/9/13.
  */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

    @transient var sc: SparkContext = _


    override def afterEach() {
        resetSparkContext()
        super.afterEach()
    }

    def resetSparkContext() {
        LocalSparkContext.stop(sc)
        sc = null
    }

}

object LocalSparkContext {
    def stop(sc: SparkContext) {
        if (sc != null) {
            sc.stop()
        }
        System.clearProperty("spark.driver.port")
    }

    /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
    def withSpark[T](sc: SparkContext)(f: SparkContext => T): T = {
        try {
            f(sc)
        } finally {
            stop(sc)
        }
    }

    def clearLocalRootDirs(): Unit = {
       // SparkUtils.clearLocalRootDirs()
    }
}
