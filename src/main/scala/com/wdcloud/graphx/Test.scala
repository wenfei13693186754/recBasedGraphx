package com.wdcloud.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject

/**
 * 本测试代码包含测试题中的三道题，全部使用scala实现，如下
 */
object Test {

  val conf = new SparkConf().setAppName("pregel").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //第一题
    val json = "{ \"a\": 1, \"c\": { \"a\": 1, \"b\": { \"c\": 2, \"d\": [3,4] } },\"b\": { \"c\": 2, \"d\": [3,4] } }"
    val jhr = jsonHandle(JSON.parseObject(json))
    println("第一题输入："+json+"\n输出是："+jhr)

    //第二题
    val data = Array[HashMap[String, String]](HashMap("a" -> "a", "c" -> "c", "d" -> "d"), HashMap("b" -> "b"))
    val text = store(data)
    println("第二题保存后的数据是："+text)

    val str = "key1=value1;key2=value2\nkeyA=valueA"
    val arr = load(str)
    println("第二题加载进来的数据是："+arr.mkString(","))

    //第三题
    lengthPath()
  }

  /**
   * 问题：
   * 	假设现在有一个有向无环图，每个节点上都带有正数权重。我们希望找到一条最优路径，使得这个路径上经过的节点的权重之和最大。
   * 	输入：n个节点，m个路径，起点
   * 	输出：最优路径的权重值之和
   *
   * 思路：
   * 	本身这个问题就是图计算方面的问题，所以采用spark graphx 的pregel算法来处理这个问题。
   * 具体思路如下：
   * 		pregel算法接收三个函数，分别是vprog、sendMsg和mergeMsg，其中vprog函数用来更新顶点本身上的属性，sendMsg用来发送消息，mergeMsg用来聚合发送到各个节点上的消息。
   * 所以我们可以这么做：
   *    1.所有节点上都放着类型是Map[String, Int]的属性，其中key是路径，value是路径对应的权重值（这里采用map的原因是，一些顶点会有多条路径同时经过，所以需要用多个键值对分别来记录每个路径机器权重值）。
   *    2.定义vprog函数，用来将收到的数据中的key拼上本节点的id，value累加上本节点的value，然后将其放到自己的属性中。
   *    	这里需要注意的是，要将初始化的时候的处理逻辑和之后的处理逻辑分开，因为初始化什么都不需要做，但是之后要对数据进行合并
   *    3.定义sendMsg函数，这里需要处理三个问题，
   *    	a. srcVertex是否具有发送消息的资格
   *    		判断是否是具有发送消息的资格，可以根据srcVertex的属性中的各个key，如果有哪个key不是以初始节点id开头的并且key没有经过累加，那么该节点就不能发送消息；
   *    	b. 闭环问题的处理，防止出现闭环，导致死循环
   *    		判断是否达到了闭环的临界点，根据dstVertex的id是否包含在srcVertex的属性中的key中就可以了，如果包含，那么就到了临界点，那么就不可以再朝这个路径发送消息。
   *    	c. 发送什么消息
   *    4.mergeMsg函数，用来将收到的消息进行聚合，这里比较简单，直接将收到的各个map取并集就可以了。
   * 最终实现如下
   */
  def lengthPath() {
    //Create an RDD for the vertices
    val vertex: RDD[(VertexId, (Map[String, Int]))] = sc.parallelize(Array((1L, Map("1" -> 1)), (2L, Map("2" -> 2)), (3L, Map("3" -> 2))))
    //Create an RDD for Edges
    val edges: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, ""), Edge(2L, 3L, ""), Edge(3L, 1L, "")))
    //Define a default user in case there are relationship with missing user
    val graph: Graph[Map[String, Int], String] = Graph(vertex, edges)

    //初始顶点
    val initV = 1 + ""

    val pGraph = graph.pregel(Map[String, Int](), Int.MaxValue, EdgeDirection.Out)(
      (id, oldValue, newValue) => // Vertex Program
        if (newValue.size == 0) { //初始化
          oldValue
        } else { //不是初始化的话，给收到的数据中的key拼上本节点的id，value累加上本节点的value
          newValue.map(x => (x._1 + id, x._2 + oldValue.get(id + "").getOrElse(0))).++:(oldValue)
        },
      triplet => { //发送消息的函数   如果本节点收到了消息成了活跃节点，那么检查src节点中的value(Map[String, Int])中的key是否都是以初始节点为开头的
        var boo = true
        val dstId = triplet.dstId + ""
        triplet.srcAttr.foreach { x =>
          //这里x._1.contains(dstId)判断实现了对闭环问题的处理，防止出席死循环
          if ((!x._1.startsWith(initV) && x._1.length() != 1) || x._1.contains(dstId)) boo = false
        }
        if (boo == true) {
          Iterator((triplet.dstId, triplet.srcAttr.filter(x => x._1.startsWith(initV))))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a.++(b) // Merge Message：
      )
    //获取每个节点上最长路径及其值
    val vlData = pGraph.vertices.mapValues(x => x.maxBy(x => x._2))
    //从所有节点属性中获取初始点最长的路径（权重之和最大）及其值
    val wValue = vlData.map(_._2).max()
    println("初始节点是：" + initV + " 与初始节点的最优路径是：" + wValue._1 + "  其权重值是：" + wValue._2)
  }

  /**
   * 将数组元素按照规则解析好后保存为字符串格式
   *
   * @param data 接收到的参数
   * @return 将接收到的参数按照规则解析完成组成的字符串
   * @author hadoop
   */
  def store(data: Array[HashMap[String, String]]): String = {
    //预定义个字符串
    var text = ""
    //对接收到的数组进行遍历解析
    data.foreach { x =>
      //如果是遍历出的第一个数组元素，那么此时text上没有拼接任何字符串，所以长度是0，此时将遍历出的数据拼接到text上  
      if (text.length() == 0) {
        for ((k, v) <- x) {
          text = text + k + "=" + v + ";"
        }
      } else { //如果遍历出的不是数组的第一个元素，那么text上已经有拼接上的字符串了，此时要拼接上换行符“\n”
        for ((k, v) <- x) {
          text = text + "\n" + k + "=" + v + ";"
        }
      }
    }
    text
  }

  /**
   * 解析给定字符串为Array[HashMap[String, String]]格式
   *
   * @param data  接收到的字符串参数
   * @return 解析处理完成的 数组
   * @author hadoop
   */
  def load(data: String): Array[HashMap[String, String]] = {
    //声明并初始化一个空的数组
    var arr: Array[HashMap[String, String]] = Array()

    //对入参按照换行符分割后遍历
    data.split("\n").map { x =>
      val hm = HashMap[String, String]()
      //解析出所有的kv元素 ，此时解析出的kv元素是"k=v"格式的，不符合我们要求 
      val kvArr = x.split(";")
      //再解析kv元素并将其放到hashMap中
      kvArr.foreach { x =>
        val arr = x.split("=")
        hm.+=(arr(0) -> arr(1))
      }
      hm
    }
  }

  /**
   * JSON串格式转化
   * 思路，因为json串的嵌套是多层的，所以考虑使用递归操作
   */
  var nj = new JSONObject() //预定义一个空的json串，用来放解析后的json串
  def jsonHandle(json: JSONObject): JSONObject = {
    //将待解析json串转化为迭代器进行迭代
    val iter1 = json.entrySet().iterator()
    while (iter1.hasNext()) {
      val entry = iter1.next()
      val key = entry.getKey

      //如果迭代出的value值不是JSONObject，那么将对应的key和value放到nj中
      if (!entry.getValue.isInstanceOf[JSONObject]) {
        nj.put(key, entry.getValue)
      } else { //如果迭代出的value值是JSONObject，那么继续迭代出该JSONObject的所有键值，然后将当前JSONObject的所有key(k2)拼接上上一级的key(k1)值后(得到k1.k2),按照"k1.k2":value的格式放到新定义的临时JSONObject中(也就是nj2)，
        val nj2 = new JSONObject()
        val jobj = entry.getValue.asInstanceOf[JSONObject]
        val iter2 = jobj.entrySet().iterator()
        while (iter2.hasNext()) {
          val kv = iter2.next()
          nj2.put(key ++ "." + kv.getKey, kv.getValue)
        }

        //递归调用该方法
        jsonHandle(nj2)
      }
    }
    nj
  }
}
