package com.linuxense.javadbf.spark

import java.io.File
import java.nio.charset.Charset

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import com.linuxense.javadbf.spark._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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

    //path = "file:///H://后台业务系统//代码--spark//data//SJSMX10807.DBF"
   //path = "file:///H://后台业务系统//代码--spark//data//zqye.dbf"
    //path = filePath+"0904保证金日结表.DBF"
    path = filePath+"users//tangp//Documents//后台业务系统//svn//2.DevelopmentDoc//2.1CustomerRequirement//业务梳理--整合版//清算文件//股息红利差别化扣税//20160426//zsmx.dbf"
    path =  "file:///D://users//tangp//Documents//后台业务系统//svn//2.DevelopmentDoc//2.1CustomerRequirement//业务梳理--整合版//清算文件//ETF//深A_跨市场ETF//跨市场ETF(实物)//申购//t+2//sjsjg0611_深登优化2期前.dbf"
    val s = sparkSession.loadAsDF(path,charset,partitionNum)
     s.show()
    s.printSchema()
    //s.collect()
   // println(s.count())
  }

  def getDBF(dirctory:File,dbfFileMap:mutable.Map[String,File]): mutable.Map[String,File] ={
    if(dirctory.isFile && (dirctory.getName.endsWith(".dbf")|| dirctory.getName.endsWith(".DBF")) ){
      dbfFileMap += ("file:///"+dirctory.toString.replace("\\","//") ->dirctory)
    }else if(dirctory.isDirectory){
      dirctory.listFiles().foreach(f=>{
        getDBF(f,dbfFileMap)
      })
    }
    dbfFileMap
  }


  test("dirctory"){
    val base = new File("D:\\users\\tangp\\Documents\\后台业务系统\\svn\\2.DevelopmentDoc\\2.1CustomerRequirement\\业务梳理--整合版\\清算文件")
    val result  = mutable.Map[String,File]()
    getDBF(base,result)
    val successFiles = mutable.Map[String,Long]()
    val failedFiles = mutable.ListBuffer[String]()
    result.foreach(path=>{
      try {
        val s = sparkSession.loadAsDF(path._1, charset, partitionNum)
        successFiles += (path._1->s.count())

      } catch {
        case e:Exception =>
          e.printStackTrace()
          failedFiles += (path._1)

      }
    })
    println(s"共${result.size}个文件：")
    println(s"读取成功${successFiles.size}个：${successFiles}")
    println(s"读取失败${failedFiles.size}个：${failedFiles.mkString(",")}")
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
        println(v)
        //println(v.asInstanceOf[RjbBean].toString)
      })
    })
    println(s"sum:${s.count()}")

  }

  test("kdfjkajf"){
   val s = "zs"
   s match {
     case "wa"=>println("===")
     case "ls"|"zs"=>println("s")
   }
  }

}
