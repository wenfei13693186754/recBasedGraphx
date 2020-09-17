package com.wdcloud.graphx.environmentContext

import scala.collection.mutable.Map

/**
 * 推荐引擎运行的上下文
 */
abstract class DataContext(var userConf: Map[String, String]) extends Serializable{
  
}  