import com.xiaomi.infra.galaxy.blobstore.hadoop.BlobInfo

/**
  * Copyright 2017, Xiaomi.
  * All rights reserved.
  * Author: haxiaolin@xiaomi.com
  */
class FDSObjectInfoWritable(o: String, s: Long, b: BlobInfo) {
  private[this] var _objectKey: String = o
  private[this] var _fileId: Long = b.getFileId
  private[this] var _blobId: String = b.getBlobId
  private[this] var _size: Long = s
  private[this] var _start: Long = b.getStart
  private[this] var _length: Long = b.getLen

  private def objectKey: String = _objectKey

  private def objectKey_=(value: String): Unit = {
    _objectKey = value
  }

  private def fileId: Long = _fileId

  private def fileId_=(value: Long): Unit = {
    _fileId = value
  }

  private def blobId: String = _blobId

  private def blobId_=(value: String): Unit = {
    _blobId = value
  }

  private def size: Long = _size

  private def size_=(value: Long): Unit = {
    _size = value
  }

  private def start: Long = _start

  private def start_=(value: Long): Unit = {
    _start = value
  }

  private def length: Long = _length

  private def length_=(value: Long): Unit = {
    _length = value
  }

}
