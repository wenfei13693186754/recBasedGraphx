package com.wdcloud.graphx.modelBuild.graph

import akka.event.slf4j.Logger
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import com.wdcloud.graphx.javaUtil.Configuration
import org.apache.spark.Accumulable
import com.wdcloud.graphx.scalaUtil.OtherUtil
import org.apache.spark.rdd.RDD
import com.wdcloud.graphx.pojo.UserMapInfo


/**
 * 数据源是txt文件的时候，源数据的读取
 */
object ReadDataFromTxt extends Serializable {
  
  val logger = Logger(this.getClass.getName)

  var namespace: String = null
  
  var userConf: Configuration = null

  def readGraphData(sc: SparkContext, conf: Configuration): RDD[(Long, Long, Double, Map[Int, Any], Map[Int, Any])] = {
    
    userConf = conf
    
    //维护了顶点业务id和innerId的映射关系的累加器
    var accPointIdMap: Accumulable[HashMap[Long, String], (Long, String)] = sc.accumulableCollection(HashMap[Long, String]())

    //维护了顶点类型和顶点类型Id之间的映射关系
    var accPointTypeMap: Accumulable[HashMap[Long, String], (Long, String)] = sc.accumulableCollection(HashMap[Long, String]())

    //从内测中读取源数据的路径
    val path = userConf.get("data.input.path")+"\\edges测试.txt"

    val graphData = sc.textFile(path, 48).map { x =>
      //january 051920c74e374a02871666a943505fd1 cd5d508d6a324a37941142db9145ea6c 01 10 1132133144693 1490601100054
      val lines = x.split(" ")
      val srcId = lines(1)
      val dstId = lines(2)
      var dst_category = lines(3)
      val bhv_type = lines(4)
      val last_time = lines(5)
      var src_category: String = null
      //对顶点类型没有给出的处理
      if (src_category == null && dst_category != null) { //如果用户没有设置src顶点的类型，设置了dst顶点的类型，那么设置src顶点类型默认值为person
        src_category = "01"
      } else if (src_category != null && dst_category == null) { //如果用户设置src顶点的类型，设没有置dst顶点的类型，那么设置dst顶点类型默认值为item
        dst_category = "03"
      } else if (src_category == null && dst_category == null) { //如果用户没有设置src顶点和dst顶点类型，那么设置所有顶点默认类型为point
        src_category = "00"
        dst_category = "00"
      } else {
      }

      //建立业务id和innerId之间的映射关系
      var src_innerId: Long = OtherUtil.hashId(src_category, srcId)
      var dst_innerId: Long = OtherUtil.hashId(dst_category, dstId)
      accPointIdMap.add(src_innerId -> srcId)
      accPointIdMap.add(dst_innerId -> dstId)

      //建立顶点类型和顶点类型id映射关系
      val userTypeLong = OtherUtil.hashId(src_category, "type")
      val itemTypeLong = OtherUtil.hashId(dst_category, "type")
      accPointTypeMap.add(userTypeLong -> src_category)
      accPointTypeMap.add(itemTypeLong -> dst_category)

      var pAttr: Map[Int, Any] = Map(1 -> userTypeLong)
      var iAttr: Map[Int, Any] = Map(1 -> itemTypeLong)

      //读取行为类型所占权重
      val relaScore: Double = userConf.get(bhv_type).toDouble
      //计算顶点之间亲密度
      val totalScore: Double = math.log10(relaScore * 1000000 / math.pow(last_time.toLong / 360000000, 1.5) + 1)
      (src_innerId, dst_innerId, totalScore, pAttr, iAttr)
    }
    
    UserMapInfo.pointIdMap = accPointIdMap.value
    UserMapInfo.pointTypeMap = accPointTypeMap.value
    
    graphData
  }
}















