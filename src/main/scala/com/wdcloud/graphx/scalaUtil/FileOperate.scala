package com.wdcloud.graphx.scalaUtil

import java.io.File
import org.apache.spark.rdd.RDD
import akka.event.slf4j.Logger
import java.io.PrintStream
import java.io.FileOutputStream
import org.apache.spark.graphx.Graph
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap
import org.apache.spark.graphx.VertexId
import scala.collection.mutable.Map

/**
 * 对文件的操作
 */
object FileOperate extends Serializable{
  
  val logger = Logger(this.getClass.getName)
  
  
  /**
   * 将推荐结果持久化到text文件
   */
  def saveResultToText(path: String, recResult: RDD[(Int, String, Map[String, Array[(String, Double)]])]){
    
	  val fs = new FileOutputStream(
			  new File(path));
	  val p = new PrintStream(fs);
    val result = DataToJson.recResultRDDMapToJson(recResult)
    
    result.collect().foreach(x => p.println(s"应用 ${x._1} 的推荐结果是：用户${x._2}的推荐结果是--》${x._3}"))
  }
  
  def graphPrintln(graph: Graph[Int2ObjectArrayMap[Object], Double]) {
	  val fs = new FileOutputStream(
			  new File("D:\\workplace-scala\\recBasedGraphx\\src\\main\\resource\\userConf\\result.txt"));
	  val p = new PrintStream(fs);

    graph.vertices.collect().foreach(x => p.println(s"顶点${x._1} 类型是：${x._2.get(1).toString()}  邻居id是：${x._2.get(2).asInstanceOf[Array[VertexId]].mkString(",")}"))
  }  
}