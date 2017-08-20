package com.xiaomi.infra.galaxy.fds.spakcleaner.bean

import com.xiaomi.infra.galaxy.blobstore.hadoop.BlobInfo

/**
  * Created by shanjixi on 17/8/9.
  */
case class BlobInfoBean(fileId:Long,blobId:String,start:Long,length:Long)
case class FDSObjectInfoBean(objectKey:String,size:Long,blobInfo:BlobInfoBean)
