package com.wdcloud.graphx.scalaUtil

import com.google.common.hash.Hashing

/**
 * 此类用来将用户的业务id转化为图计算需要用的long类型的id
 */
object UserIdToLong {
  
  /**
   * 将用户的业务id转化为计算需要的long类型的id
   */
  def userIdToLong(userIdListStr: List[String]): List[Long] = {
    userIdListStr.map { x => hashId("person", x) }
  }  
  
  /*
   * 标识不同物品id的工具方法
   */
  private def hashId(name: String, str: String): Long = {
	  Hashing.md5().hashString(name + "" + str).asLong()
  }
def main(args: Array[String]): Unit = {
  println(hashId("person","person_2"))
}
}