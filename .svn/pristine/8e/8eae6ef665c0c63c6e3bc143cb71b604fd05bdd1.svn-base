package com.wdcloud.graphx.resultHandle.graph

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
/**
 * 保存推荐结果
 */
trait SaveResult{
  
  def saveResult(result: RDD[(Int, String, Map[String, Array[(String, Double)]])])
}