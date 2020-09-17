package com.wdcloud.graphx.scalaUtil

import com.google.common.hash.Hashing

object OtherUtil {
  /**
   * 标识不同物品id的工具方法
   */
  def hashId(name: String, str: String) = {
    Hashing.md5().hashString(name + "" + str).asLong()
  }
}