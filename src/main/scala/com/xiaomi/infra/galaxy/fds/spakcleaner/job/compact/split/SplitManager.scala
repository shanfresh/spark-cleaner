package com.xiaomi.infra.galaxy.fds.spakcleaner.job.compact.split

import com.xiaomi.infra.galaxy.blobstore.hadoop.FileManager
import com.xiaomi.infra.galaxy.fds.spakcleaner.bean.FdsFileStatus

/**
  * Created by haxiaolin on 17/9/12.
  */
case class Split(fileStatus: List[FdsFileStatus]=List.empty){
    var size = 0L
    def addFile(fdsFileStatus: FdsFileStatus): Unit ={
        this.size += fdsFileStatus.remainSize
    }
    def getSize():Long={
        size
    }
}
case class SplitManager(var splits:List[Split]=List.empty,var currentSplit:Split=Split()){
    val maxBytes:Long = getMaxSize
    def addFile(fdsFileStatus: FdsFileStatus):Unit={
        currentSplit.addFile(fdsFileStatus)
        if (currentSplit.getSize >= maxBytes) {
            splits = splits :+ currentSplit
            currentSplit = new Split()
        }
    }
    def merge(other: SplitManager):SplitManager={
        splits = splits ++ other.splits
        other.currentSplit.fileStatus.foreach(file=>{
            addFile(file)
        })
        this
    }
    def getAllSplitsIterator():Iterator[Split] = {
        (splits :+ currentSplit).toIterator
    }
    def getMaxSize():Long={
        //Todo: we need to read the size from spark context
        //conf.getLong(FileManager.HADOOP_BLOBSTORE_MAX_FILE_SIZE_BYTES, FileManager.DEFAULT_HADOOP_BLOBSTORE_MAX_FILE_SIZE_BYTES)
        FileManager.DEFAULT_HADOOP_BLOBSTORE_MAX_FILE_SIZE_BYTES
    }
}
