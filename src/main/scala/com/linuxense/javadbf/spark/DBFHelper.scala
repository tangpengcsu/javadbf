package com.linuxense.javadbf.spark

import java.net.URI
import java.nio.charset.Charset

import com.linuxense.javadbf.{DBFOffsetReader}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil


case class DBFHelper(path:String,
                     charset:Charset,
                     userName:String,
                     showDeletedRows:Boolean,
                     connectTimeout:Int,
                     maxRetries:Int) {
  private var fs: FileSystem = null
  private var inputStream: FSDataInputStream = null
  private var reader: DBFOffsetReader = null

  def open(): Unit = {

    val conf = SparkHadoopUtil.get.conf
    conf.set("ipc.client.connect.timeout", connectTimeout.toString) //超时时间3S - 3000
    conf.set("ipc.client.connect.max.retries.on.timeouts", maxRetries.toString) // 重试次数1
    fs = FileSystem.get(URI.create(path), conf, userName)
    inputStream = fs.open(new Path(path))
    reader = new DBFOffsetReader(inputStream, charset, showDeletedRows)
  }

  def getReader = reader

  def getRecordCount = reader.getRecordCount

  def getFields=reader.getFields

  def close(): Unit ={
    IOUtils.closeQuietly(reader)
    IOUtils.closeQuietly(inputStream)
    IOUtils.closeQuietly(fs)
  }

}
