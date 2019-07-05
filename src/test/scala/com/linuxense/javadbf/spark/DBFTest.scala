package com.linuxense.javadbf.spark

import java.nio.charset.Charset

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import com.linuxense.javadbf.spark._
class DBFTest extends FunSuite{

  val conf = new SparkConf()
    .setAppName("IgniteRDDExample")
    .setMaster("local[2]")
    .set("spark.executor.instances", "2")
  val filePath = "file:///D://"
  var path = filePath+"SJSJG.DBF"

  val partitionNum =36
  val charset = Charset.forName("GBK")
  // Spark context.
  val sparkSession:SparkSession= SparkSession.builder().config(conf).getOrCreate()

  test("dataframe"){
    val filePath = "file:///D://"
    path = "file:///H://后台业务系统//代码--spark//data//SJSMX10807.DBF"
   // path = "file:///H://后台业务系统//代码--spark//data//zqye.dbf"
    //path = filePath+"0904保证金日结表.DBF"
    val s = sparkSession.loadAsDF(path,charset,partitionNum)
    //s.show()
    //s.collect()
    println(s.count())
  }
  test("row"){
    val s = sparkSession.loadAsRowRDD(path,charset,partitionNum)
    s.foreach(i=>{
      println(i)
    })
  }
  test("read"){
    // path = "file:///D://jsmx13.dbf"
    val filePath = "file:///D://"
   // path = filePath+"0904机构费用明细.dbf"
   // path = filePath+"0904交收后成交汇总表.DBF"
   // path = filePath+"0904交易一级清算表.DBF"
    //path = filePath+"0904保证金日结表.DBF"
    path = filePath+"0904保证金日结表.DBF"
    //path = filePath+"jsmx13.dbf"
   // path = "file:///H://后台业务系统//清算文件//SJSMX10901.DBF"
    val optParam = List(DBFOptDFParam("settDate",20180808),DBFOptDFParam("settBatNo",1),DBFOptDFParam("sn",null,true))


     //val s = sparkSession.sparkContext.loadAsRowRDD(path,charset,partitionNum,optParam)

   val s = sparkSession.loadAsBeanRDD[CaseRjbBean](path,charset,partitionNum,optParam)
/* val col= s.mapPartitionsWithIndex((p,d)=>{

   List((p,d.size)).iterator
 }).collect()
    println(s"=====sum:${s.count()}-${col.mkString(",")}")*/
    s.foreachPartition(i=>{
      i.foreach(v=>{
        println(v.asInstanceOf[CaseFile])
        //println(v.asInstanceOf[RjbBean].toString)
      })
    })
    println(s"sum:${s.count()}")
    println(s"fdsf:${List(1)}")
  }

  test("kdfjkajf"){
    val left = Array(1,2,3)
    val z = 0 +: left    //List(0,1,2,3)
    println(z)

  }

}
