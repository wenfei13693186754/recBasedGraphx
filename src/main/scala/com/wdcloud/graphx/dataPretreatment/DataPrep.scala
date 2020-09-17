package com.wdcloud.graphx.dataPretreatment

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.SparkContext
import com.wdcloud.graphx.javaUtil.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import com.wdcloud.graphx.scalaUtil.HbaseUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.TableName
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import akka.event.slf4j.Logger

/**
 * 将原始行为数据进行预处理，使得数据成为图计算使用的数据
 */
object DataPrep extends Serializable {

  private val logger = Logger(this.getClass.getName)

  def dataPrep(sc: SparkContext, conf: Configuration): String = {
    val namespace = conf.get("namespace")
    val tableName = conf.get("user.behavior.table")
    val oldBhvTable = namespace+":"+tableName
    val newBhvTable = namespace+":NEW_"+tableName
    val bhvInfo = readOldBhvTable(sc, oldBhvTable)
    writeNewBhvTable(bhvInfo, newBhvTable)
    logger.warn("数据预处理结束")
    newBhvTable
  }

  /*
   * 1.对原始行为表进行复制，得到我们的操作表oper_table;
   * 1.读取oper_table数据中的ACTION列；
   * 2.将行为转化为对应的权重值；
   * 3.将转化后的数据重新写入到oper_table表中
   */
  private def readOldBhvTable(@transient sc: SparkContext, oldBhvTable: String): RDD[(String, Array[(String, String)])] = {

    //生成hbaseConf
    val hbaseConf = HbaseUtil.createHbaseConfObject()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, oldBhvTable)
    hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "INFO")

    val admin = new HBaseAdmin(hbaseConf)
    //判断表是否可用
    if (admin.isTableAvailable(oldBhvTable)) {
      val hBaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      val bhvInfo = hBaseRDD.map { //这里是按行键读取的，也就是下边获得的key是一个一个的行健，不是一个行健组成的数组   行健：用户业务id_物品业务id
        x =>
          val result = x._2

          val row = result.rawCells() //返回支持此Result实例的Cells组成的数组
          val rowKey = Bytes.toString(result.getRow)
          val cellArray = row.map { cell =>
            var key = Bytes.toString(cell.getQualifier)
            val value = Bytes.toString(cell.getValue)
            if (key == "ACTION") {
              val weight = actionMap(value)+""
              (key, weight)
            } else {
              (key, value)
            }
          }

          (rowKey, cellArray)
      }

      admin.close()
      bhvInfo

    } else {
      System.exit(1)
      logger.error("行为表${bhvTable}不可用")
      throw new IllegalArgumentException
    }

  }

  /**
   * 将经过处理的用户行为数据写入到另一张行为表中
   * @param info  封装了处理后的用户行为信息的RDD
   * @param newBhvTable  新的用户行为表(包括了命名空间的表名)
   */
  private def writeNewBhvTable(info: RDD[(String, Array[(String, String)])], newBhvTable: String) {
    //生成hbaseConf
    val hbaseConf = HbaseUtil.createHbaseConfObject()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, newBhvTable)

    val admin = new HBaseAdmin(hbaseConf)
    //判断表是否存在，不存在则创建
    if (!admin.isTableAvailable(newBhvTable)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(newBhvTable))
      val hcd = new HColumnDescriptor("INFO")
      //add  column family to table  
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)

    }

    admin.close()

    info.foreachPartition { iter =>
      val hbaseConf = HbaseUtil.createHbaseConfObject()
      hbaseConf.set(TableInputFormat.INPUT_TABLE, newBhvTable)
      hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "INFO")

      val table = new HTable(hbaseConf, Bytes.toBytes(newBhvTable))
      table.setAutoFlush(false, true)
      table.setWriteBufferSize(100000)

      /*一个Put对象就是一行记录，在构造方法中指定主键  
       * 因为hbase中的ImmutableBytesWritable类没有实现Serializable接口，所以不能序列化，所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换  
       * Put.add方法接收三个参数：列族，列名，数据  
       */
      iter.foreach { x =>
        val rowKey = x._1
        val cellInfo = x._2
        val put = new Put((rowKey).getBytes()); //为指定行创建一个Put操作，指定行健是用户业务id
        cellInfo.map {
          x =>
            val cellKey = x._1
            val cellValue = x._2

            put.addColumn("INFO".getBytes(), cellKey.getBytes(), Bytes.toBytes(cellValue.toString())); //将对应于某个物品类目下的推荐结果写入到某一列 
        }

        table.put(put)
      }
      table.flushCommits()
      table.close()
    }
    logger.info("预处理后的行为表数据转化成功")

  }

  /*
   * 行为类型和权值映射
   */
  def actionMap(act: String): Double = {
    act match {
      case "01" => 0.09
      case "02" => 0.07
      case "03" => 0.07
      case "04" => 0.07
      case "05" => 0.07
      case "06" => 0.07
      case "07" => 0.07
      case "08" => 0.07
      case "09" => 0.07
      case "10" => 0.05
      case "11" => 0.05
      case "12" => 0.05
      case "13" => 0.05
      case "14" => 0.05
      case "15" => 0.05
      case "16" => 0.05
      case _    => 0
    }
  }
}