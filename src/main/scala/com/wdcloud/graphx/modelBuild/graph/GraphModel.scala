package com.wdcloud.graphx.modelBuild.graph

import com.wdcloud.graphx.javaUtil.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import scala.collection.mutable.Map
trait GraphModel{  
  
  /*
   * 创建图抽象方法
   */
  def createGraph(sc: SparkContext, conf: Configuration): Graph[Map[Int, Any], Double]
}   