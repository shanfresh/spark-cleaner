package com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.core

import org.apache.spark.Partitioner
import java.util
import java.util.Comparator
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Partitioner
/**
  * Created by haxiaolin on 17/8/20.
  */
class BulkLoadPartitioner(startKeys:Array[Array[Byte]]) extends Partitioner {
    // when table not exist, startKeys = Byte[0][]
    override def numPartitions: Int = if (startKeys.length == 0) 1 else startKeys.length

    override def getPartition(key: Any): Int = {

        val comparator: Comparator[Array[Byte]] = new Comparator[Array[Byte]] {
            override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
                Bytes.compareTo(o1, o2)
            }
        }

        val rowKey:Array[Byte] =
            key match {
                case qualifier: KeyFamilyQualifier =>
                    qualifier.rowKey
                case _ =>
                    key.asInstanceOf[Array[Byte]]
            }
        var partition = util.Arrays.binarySearch(startKeys, rowKey, comparator)
        if (partition < 0)
            partition = partition * -1 + -2
        if (partition < 0)
            partition = 0
        partition
    }
}
