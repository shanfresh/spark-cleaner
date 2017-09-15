package com.xiaomi.infra.galaxy.fds.spakcleaner.cleaner

import com.xiaomi.infra.galaxy.fds.spakcleaner.basic.SharedSparkContext
import org.scalatest.FunSuite

/**
  * Created by haxiaolin on 17/9/13.
  */
class TestAggregator extends FunSuite with SharedSparkContext{
    test("test initializing spark context") {
        val list = List(1, 2, 3, 4)
        val rdd = sc.parallelize(list)
        assert(rdd.count === list.length)
    }
}
