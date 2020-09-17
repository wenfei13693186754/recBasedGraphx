package com.wdcloud.graphx.scalaUtil

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection
import akka.event.slf4j.Logger
import scala.Iterator
import org.apache.spark.graphx._
import com.google.common.hash.Hashing

object CreateSubGraph extends Serializable {
  
  val logger = Logger(this.getClass.getName)
  /**  
   * 生成子图
   */
  def createSubGraph(
      graph: Graph[Map[String, Object], Double],
      userList: List[VertexId]
  ): Graph[Map[String, Object], Double] = {
    val rand = Math.random()+""
    val PregelUtil = new PregelUtil()
    val g = PregelUtil.apply(graph, "", 3, EdgeDirection.Either)(
      (vId, oldAttr, newAttr) =>
        if (PregelUtil.iterNum == 0) { //初始化

          oldAttr.+("sign" -> newAttr)
        } else if (PregelUtil.iterNum == 3) { //第三次迭代
            
          oldAttr.updated("sign", newAttr)
        } else if (PregelUtil.iterNum == 2) { //第二次迭代
        	oldAttr.updated("sign", newAttr)
        } else { //第一次迭代
          
          oldAttr.updated("sign", newAttr)
        },

      triplet =>
        //初始化后第一次迭代
        if (PregelUtil.iterNum == 0) {
          //限定固定的用户顶点
          if (userList.contains(triplet.srcId)) {
            Iterator((triplet.dstId, rand))
          } else {
            Iterator.empty
          }
          //第二次迭代
        } else if (PregelUtil.iterNum == 1) {
          if (triplet.dstAttr.apply("type").asInstanceOf[String].equals("circle")&&triplet.dstAttr.apply("sign").asInstanceOf[String].equals(rand)) {
            
            Iterator((triplet.srcId, triplet.dstAttr.apply("sign").asInstanceOf[String]))
          } else if(triplet.srcAttr.apply("sign").asInstanceOf[String].equals(rand)){
            Iterator((triplet.dstId, triplet.srcAttr.apply("sign").asInstanceOf[String]))
          }else{
            Iterator.empty
          }
          //第三次迭代 
        } else if (PregelUtil.iterNum == 2) {

            Iterator((triplet.dstId, triplet.srcAttr.apply("sign").asInstanceOf[String]))
        } else {

          Iterator.empty
        },
      (data1, data2) => data1)
    logger.warn("子图提取成功")
    g.subgraph(vpred = (id, attr) => rand.equals(attr.apply("sign").asInstanceOf[String]))
  }
  
}