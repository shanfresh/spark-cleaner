package com.xiaomi.infra.galaxy.fds.spakcleaner.bean

/**
  * Created by haxiaolin on 17/8/10.
  */
case class FdsFileStatus(file_id: Long, emptyPercent: Int, remainSize: Long, path: String, deleted: Boolean = false)
