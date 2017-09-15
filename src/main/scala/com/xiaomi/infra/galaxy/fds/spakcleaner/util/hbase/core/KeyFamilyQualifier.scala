package com.xiaomi.infra.galaxy.fds.spakcleaner.util.hbase.core

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by haxiaolin on 17/8/20.
  */
class KeyFamilyQualifier(val rowKey:Array[Byte], val family:Array[Byte], val qualifier:Array[Byte])
        extends Comparable[KeyFamilyQualifier] with Serializable {
    override def compareTo(o: KeyFamilyQualifier): Int = {
        var result = Bytes.compareTo(rowKey, o.rowKey)
        if (result == 0) {
            result = Bytes.compareTo(family, o.family)
            if (result == 0) result = Bytes.compareTo(qualifier, o.qualifier)
        }
        result
    }
    override def toString: String = {
        Bytes.toString(rowKey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
    }
}
