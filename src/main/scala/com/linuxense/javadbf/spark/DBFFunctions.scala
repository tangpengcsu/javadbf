package com.linuxense.javadbf.spark

import java.nio.charset.Charset

import com.linuxense.javadbf.{DBFField}
import com.linuxense.javadbf.spark.Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{Partition}

import scala.reflect.ClassTag

class DBFFunctions(@transient val sparkSession: SparkSession) extends Serializable {
  def loadAsDF(path: String, charset: Charset,
               partitionNum: Int,
               snName: String = "SN",
               showDeletedRows: Boolean = false,
               userName: String = "hadoop",
               connectionTimeout: Int = 3000, maxRetries: Int = 1): DataFrame = {
    val dbfHelper = DBFHelper(path, charset, userName, showDeletedRows, connectionTimeout, maxRetries)
    try {
      dbfHelper.open()

      val schema = buildSchema(dbfHelper.getFields, snName)

      val rdd = loadAsRowRDD(path, charset, partitionNum, Nil, showDeletedRows, userName, connectionTimeout, maxRetries)
      sparkSession.createDataFrame(rdd, schema)
    } finally {
      dbfHelper.close()
    }

  }

  def loadAsRowRDD(path: String, charset: Charset,
                   partitionNum: Int,
                   param: List[DBFOptParam] = Nil,
                   showDeletedRows: Boolean = false,
                   userName: String = "hadoop",
                   connectionTimeout: Int = 3000, maxRetries: Int = 1): RDD[Row] = {

    val partitions = 0 until (partitionNum) map {
      DBFPartition(_).asInstanceOf[Partition]
    } toArray

    new DBFReaderRDD[Row, DBFOptParam, DBFRowTransferParam](sparkSession.sparkContext, path, DBFRowHandler(), charset.name(), showDeletedRows, userName, connectionTimeout, maxRetries, partitions, param, Row.getClass)
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

    val m = implicitly[ClassTag[T]]
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    new DBFReaderRDD[Any, DBFOptDFParam, DBFBeanTransferParam](sparkSession.sparkContext, path, DBFBeanHandler(), chasrset.name(), showDeletedRows, userName, connectionTimeout, maxRetries, partitions, param, clazz).asInstanceOf[RDD[T]]
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

  private[this] def buildSchema(fields: Array[DBFField], snName: String): StructType = {
    val snField = new StructField(snName, IntegerType, false)
    val allField = snField +: fields.map(i ⇒
      new StructField(i.getName, dataType(i.getType, i.getDecimalCount), nullable = true))
    new StructType(allField)
  }


}

