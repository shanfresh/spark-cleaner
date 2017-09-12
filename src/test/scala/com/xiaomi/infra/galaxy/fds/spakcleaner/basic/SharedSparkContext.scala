package com.xiaomi.infra.galaxy.fds.spakcleaner.basic

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by shanjixi on 17/9/13.
  */
trait SharedSparkContext extends BeforeAndAfterAll with SparkContextProvider {
    self: Suite =>

    @transient private var _sc: SparkContext = _

    override def sc: SparkContext = _sc

    override def beforeAll() {
        _sc = new SparkContext(conf)
        setup(_sc)
        super.beforeAll()
    }

    override def afterAll() {
        try {
            LocalSparkContext.stop(_sc)
            _sc = null
        } finally {
            super.afterAll()
        }
    }

}
