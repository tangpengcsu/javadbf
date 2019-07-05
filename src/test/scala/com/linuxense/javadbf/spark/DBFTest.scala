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
  var path = "file:///D://SJSJG.DBF"

  val partitionNum =36
  val charset = Charset.forName("GBK")
  // Spark context.
  val sparkSession:SparkSession= SparkSession.builder().config(conf).getOrCreate()

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

   val s = sparkSession.sparkContext.loadAsBeanRDD[CaseRjbBean](path,charset,partitionNum,optParam)
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

}
