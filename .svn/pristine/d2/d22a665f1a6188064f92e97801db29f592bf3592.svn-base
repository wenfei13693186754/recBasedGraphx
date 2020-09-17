package com.wdcloud.graphx.modelBuild.graph.operate

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import akka.event.slf4j.Logger

/**
 * 对图模型进行操作，包括对图的持久化和将持久化的图获取出来
 */
class GraphOperate {
  
	val logger = Logger(this.getClass.getName)
	
  /**
   * 将图进行持久化到hdfs中
   * 
   * param:
   * 	graph: 需要持久化的图
   *  path: 持久化图的基础路径
   * return：
   * 	持久化是否成功
   *  
   */
  def saveGraph(graph: Graph[Map[String, Object], Double], path: String): Unit = {
    
    val vertexPath = path+"/vertex"
    val edgePath = path+"/edge"
    graph.vertices.saveAsObjectFile(vertexPath)
    graph.edges.saveAsObjectFile(edgePath)
    
    logger.warn("图持久化完成，持久化路径是： " + graph.vertices.count())
  }  
  
  /**
   * 从hdfs中读出持久化好的图
   * 
   * param:
   * 		sc: spark程序运行上下文
   * 		path: 持久化图的基础路径，图信息需要从这个路径下读取
   * return:
   * 		之前持久化好的图
   */
  def getGraph(sc: SparkContext, path: String): Graph[Map[String, Object], Double] = {
    val vertexPath = path+"/vertex"
    val edgePath = path+"/edge"
    val vertices = sc.objectFile[(Long, Map[String, Object])](vertexPath)
    val edges = sc.objectFile[Edge[Double]](edgePath)
    Graph(vertices, edges)
  }
    
}