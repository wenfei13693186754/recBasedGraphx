package com.wdcloud.graphx.modelTraining

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import com.wdcloud.graphx.environmentContext.DataContext
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexId
/**
 * 推荐算法对象的公共接口
 */
abstract class Recommender(userConf: Map[String, String]) extends DataContext(userConf) {
  /** 
   * param:      
   * 		Graph[Map[String, Object], Double] 
   * return:
   * 		RDD[(命名空间，用户业务id, Map[推荐物品类型, List[(推荐物品业务id, Double)]])]
   */
  def predict(sc: SparkContext, graphModel: Graph[Map[Int, Any], Double]): RDD[(VertexId, Map[Long, Array[(Long, Double)]])]
}            