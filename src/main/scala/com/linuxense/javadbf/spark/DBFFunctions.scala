package com.linuxense.javadbf.spark

import java.nio.charset.Charset

import com.linuxense.javadbf.DBFField
import com.linuxense.javadbf.spark.Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext}

import scala.reflect.ClassTag

class DBFFunctions(@transient val sc: SparkContext) extends Serializable {

  def loadAsRowRDD(path: String, chasrset: Charset,
                   partitionNum: Int,
                   param: List[DBFOptParam] = Nil,
                   showDeletedRows: Boolean = false,
                   userName: String = "hadoop",
                   connectionTimeout: Int = 3000, maxRetries: Int = 1): RDD[Row] = {

    val partitions = 0 until (partitionNum) map {
      DBFPartition(_).asInstanceOf[Partition]
    } toArray

    new DBFReaderRDD[Row, DBFOptParam](sc, path, DBFRowHandler.process[Row], chasrset.name(), showDeletedRows, userName, connectionTimeout, maxRetries, partitions, param, Row.getClass, defaultAdjustLength)
  }

  def loadAsBeanRDD[T: ClassTag](path: String,
                                 chasrset: Charset,
                                 partitionNum: Int,
                                 param: List[DBFOptDFParam] = Nil,
                                 showDeletedRows: Boolean = false,
                                 userName: String = "hadoop",
                                 connectionTimeout: Int = 3000, maxRetries: Int = 1): RDD[T] = {

    val partitions = 0 until (partitionNum) map {
      DBFPartition(_).asInstanceOf[Partition]
    } toArray

    val clazz = getClazz[T]()

    new DBFReaderRDD[Any, DBFOptDFParam](sc, path, DBFBeanHandler.process[T], chasrset.name(), showDeletedRows, userName, connectionTimeout, maxRetries, partitions, param, clazz, defaultAdjustLength).asInstanceOf[RDD[T]]
  }


  def adjustJGMX(fields: Array[DBFField]): Unit = {
    val opt = fields.find(_.getName == "BY3")
    if (opt.isDefined) {
      val f = opt.get
      val idx = fields.indexOf(f)
      f.setLength(50)
      fields(idx) = f
    }
  }

  def defaultAdjustLength(fields: Array[DBFField]) = {

  }




}

