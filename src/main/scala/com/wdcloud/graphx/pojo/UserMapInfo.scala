package com.wdcloud.graphx.pojo

import scala.collection.mutable.HashMap

/**
 * 用来保存用户信息的映射关系
 */
object UserMapInfo {
  //维护了顶点业务id和innerId映射关系
  var pointIdMap: HashMap[Long, String] = null

  //维护了顶点类型和顶点类型的innerId之间的关系    
  var pointTypeMap: HashMap[Long, String] = null

  //定义顶点属性映射关系
  val pointAttrMap: Map[String, Int] = Map("type" -> 1, "neiborId" -> 2, "rec" -> 3)
}