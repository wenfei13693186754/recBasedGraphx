package com.wdcloud.graphx.scalaUtil

import scala.reflect.ClassTag
import java.lang.reflect.Method
import org.apache.spark.SparkContext

/**
 * 反射创建对象，调用方法
 */
object ScalaRefUtil {

  /**
   * 构造函数中有一个参数的，使用反射生成对象
   *  @param className 类的全名
   *  @param classParam 类构造函数的形参
   * 	@return Any 指定类的指定构造方法创建出来的对象
   */
  def newInstance(className: String, classParam: Object): Any = {
    try {
      val clazz = Class.forName(className)
      val cons = clazz.getConstructors
      cons(0).newInstance(classParam)
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
  }

  /**
   * 对于构造函数中没有形参的类，使用反射生成对象
   *  @param className 类的全名
   * 	@return Any 指定类的指定构造方法创建出来的对象
   */
  def newInstance(className: String): Any = {
    try {
      val clazz = Class.forName(className)
      clazz.newInstance()
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
  }

}

