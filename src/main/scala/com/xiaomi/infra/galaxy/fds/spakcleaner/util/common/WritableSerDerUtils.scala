package com.xiaomi.infra.galaxy.fds.spakcleaner.util.common
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import org.apache.hadoop.io.Writable

/**
  * Created by haxiaolin on 17/9/15.
  * This Utils is used for random Access HBASE.  sc.newAPIHadoopRDD() doesn't use this class
  */
object WritableSerDerUtils {
    def serialize(writable: Writable): Array[Byte] = {
        val out = new ByteArrayOutputStream()
        val dataOut = new DataOutputStream(out)
        writable.write(dataOut)
        dataOut.close()
        out.toByteArray
    }

    def deserialize(bytes: Array[Byte], writable: Writable): Writable = {
        val in = new ByteArrayInputStream(bytes)
        val dataIn = new DataInputStream(in)
        writable.readFields(dataIn)
        dataIn.close()
        writable
    }
}
