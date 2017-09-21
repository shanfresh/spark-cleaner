package com.xiaomi.infra.galaxy.fds.spakcleaner.job.compactor

import com.xiaomi.infra.galaxy.fds.spakcleaner.util.HDFS.HDFSPathFinder
import org.junit.{Assert, Test}

/**
  * Created by shanjixi on 17/9/17.
  */
class HDFSPathFinderTest {
    @Test
    def testGetAggregatorTimestampFromHDFSFile():Unit={
        val cluster_name="1234"
        val fds_file_cleaner_base_path = "hdfs://${cluster_name}/home/operator/fdscleaner/aggregator/"
        val fds_file_aggregator_file_status_path  = "2017-09-17/1505661514000/file_status"
        val fullPath = HDFSPathFinder.getAggretatorFileStatusByBaseFolerAndSubFoler(fds_file_cleaner_base_path,fds_file_aggregator_file_status_path)

        val timestamp= HDFSPathFinder.getAggregatorTimestampFromHDFSFile(fullPath)
        Assert.assertEquals(1505661514000L,timestamp)

    }

}
