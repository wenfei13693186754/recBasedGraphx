package com.wdcloud.graphx.modelTraining.graph

import java.io.FileNotFoundException
import java.io.IOException
import scala.collection.mutable.Map
import scala.collection.mutable.LinkedList
import scala.io.BufferedSource
import scala.io.Source

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import com.wdcloud.graphx.javaUtil.Configuration
import com.wdcloud.graphx.modelTraining.Recommender
import com.wdcloud.graphx.modelTraining.graph.util.GraphResultHandle

import akka.event.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
 * 为指定用户组成的list进行基于圈子的推荐和基于二度好友的推荐
 * 	param:
 * 		graph: 进行推荐的图模型
 *    namespace: 命名空间
 */
class CompositeRecByUserListRecommenders(userConf: Map[String, String]) { 

  val logger = Logger(this.getClass.getName)
  val conf: Configuration = null
  //用户指定的需要进行推荐的用户所组成的list
  var userIdListStr: Broadcast[Array[Long]] = null  
  
  /**
   * 混合基于圈子和基于二度好友的推荐
   * param:
   * 		userIdListStr: 指定用户id组成的list
   * return:
   * 		推荐结果
   */
  def predict(sc: SparkContext, graphModel: Graph[Map[Int, Any], Double]): RDD[(Int, Long, Map[Long, Array[(Long, Double)]])] = {
    
    //读取配置信息
    val namespace = userConf.get("namespace").get
      
    val graph = graphModel.cache()
    
    //基于好友的推荐，包括推荐好友、物品和圈子，返回值类型是：
    //RDD[(VertexId, (String, Map[String, Array[(String, Double)]]))]-->RDD[(用户id，(用户业务id, Map[推荐物品类型, Array[(推荐物品业务id, Double)]]))]
    val recommenderBasedFriends = new TwoDegreesFriendsRecommender(userConf)
    val recBasedFriends = recommenderBasedFriends.predict(sc, graph)
    

    logger.warn("基于好友推荐完成")
    //离线的对每个用户基于圈子进行物品、好友和圈子的推荐,返回值是RDD[(String, String, Map[String, Array[(String, Double)]])]
    val recommenderBasedcircle = new MixCommunityRecommender(userConf)  
    val recBasedCircle = recommenderBasedcircle.predict(sc, graph)
    
    //对两种推荐结果进行合并   INFO.LOGINID:112233xyf;
    val comResult = recBasedFriends.union(recBasedCircle).reduceByKey((x, y) => GraphResultHandle.mapCombine(x, y)).map(x => (0, x._1, x._2))
  
    //对结果进行过滤
    userIdListStr = sc.broadcast(readUserIdList())  
    val recResult = comResult.filter(x => userIdListStr.value.contains(x._2))  
    
    graph.unpersistVertices(blocking = false)
    graph.edges.unpersist(blocking = false)

    logger.warn("基于二度好友和基于圈子对指定list用户进行推荐成功")

    recResult
  }

  /**
   * 对userIdListStr进行初始化
   */
  def readUserIdList(): Array[Long] = {
    val dataType = userConf.get("dataSource.type").get
    if (dataType.equals("txt")) {
      val path = conf.get("data.input.path")
      var bs: BufferedSource = null
      try {
        bs = Source.fromFile(path + "\\userIdList.txt")
        val lines = bs.getLines()
        lines.mkString.split(",").map { x => Long.unbox(x) }.toArray

      } catch {
        case e: FileNotFoundException =>
          logger.error("{" + path + "} file not found")
          Array[Long]()
        case e: IOException =>
          logger.error("Got a IOException")
          Array[Long]()
      } finally {
        bs.close
      }
    } else if (dataType.equals("hbase")) {
      val path = conf.get("data.input.path")
      var bs: BufferedSource = null
      try {
        bs = Source.fromFile(path + "\\userIdList.txt")
        val lines = bs.getLines()
        lines.mkString.split(",").map { x => Long.unbox(x) }.toArray

      } catch {
        case e: FileNotFoundException =>
          logger.error("{" + path + "} file not found")
          Array[Long]()
        case e: IOException =>
          logger.error("Got a IOException")  
          Array[Long]()
      } finally {
        bs.close
      }
    } else {
      val path = conf.get("data.input.path")
      var bs: BufferedSource = null
      try {
        bs = Source.fromFile(path + "\\userIdList.txt")
        val lines = bs.getLines()
        lines.mkString.split(",").map { x => Long.unbox(x) }.toArray

      } catch {
        case e: FileNotFoundException =>
          logger.error("{" + path + "} file not found")
          Array[Long]()
        case e: IOException =>
          logger.error("Got a IOException")
          Array[Long]()
      } finally {
        bs.close
      }
    }
  }
}















