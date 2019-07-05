package com.linuxense.javadbf.spark

import java.net.URI
import java.nio.charset.Charset

import com.linuxense.javadbf.spark.Utils._
import com.linuxense.javadbf.{DBFField, DBFOffsetReader, DBFRow}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import scala.util.control.Breaks._

case class DBFPartition(idx: Int) extends Partition {
  override def index: Int = idx

}

class DBFReaderRDD[T: ClassTag, V <: DBFParam](sparkContext: SparkContext,
                                               path: String,
                                               conv: (Int, Array[DBFField], DBFRow, List[V], ru.RuntimeMirror, ru.ClassMirror, ru.MethodMirror, Iterable[Tuple2[ru.TermSymbol, Option[String]]]) => T,
                                               charSet: String,
                                               showDeletedRows: Boolean,
                                               userName: String,
                                               connectTimeout: Int,
                                               maxRetries: Int,
                                               partitions: Array[Partition],
                                               param: List[V] = Nil,
                                               clazz: Class[_],
                                               adjustFields: (Array[DBFField]) => Unit) extends RDD[T](sparkContext, deps = Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition = split.asInstanceOf[DBFPartition]
    var fs: FileSystem = null
    var inputStream: FSDataInputStream = null
    var reader: DBFOffsetReader = null
    val conf = SparkHadoopUtil.get.conf
    conf.set("ipc.client.connect.timeout", connectTimeout.toString) //超时时间3S - 3000
    conf.set("ipc.client.connect.max.retries.on.timeouts", maxRetries.toString) // 重试次数1
    try {
      fs = FileSystem.get(URI.create(path), conf, userName)

      inputStream = fs.open(new Path(path))
      reader = new DBFOffsetReader(inputStream, Charset.forName(charSet), showDeletedRows)

      adjustFields(reader.getFields()) //调整字段长度

      val recoderCount = reader.getRecordCount

      val result: mutable.ListBuffer[T] = ListBuffer()
      val (startOffset, endOffset) = DBFReaderRDD.calcOffset(recoderCount, partition.idx, partitions.size)


      if (recoderCount != 0 && startOffset != endOffset) {
        reader.setStartOffset(startOffset)
        reader.partitionIdx = partition.idx
        reader.setEndOffset(endOffset)
        val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader) //获取运行时类镜像
        val classMirror = runtimeMirror.reflectClass(runtimeMirror.classSymbol(clazz))
        val typeSignature = classMirror.symbol.typeSignature
        val theOwner = typeSignature.typeSymbol

        val constructorSymbol = typeSignature.decl(ru.termNames.CONSTRUCTOR)
          .filter(i => i.asMethod.paramLists.flatMap(_.iterator).isEmpty)
          .asMethod
        val constructorMethod = classMirror.reflectConstructor(constructorSymbol)
        //包含父类字段
        val reflectFields = typeSignature.baseClasses
          .flatMap(i => i.asClass.typeSignature.decls)
          .filter(i => i.isTerm && (i.asTerm.isVar || i.asTerm.isVal))
          .groupBy(_.name.decodedName.toString.trim)
          .map(i => {
            if (i._2.size > 1) {
              i._2.filter(_.owner == theOwner)
            } else {
              i._2
            }
          }).flatMap(_.iterator)
          .map(i => {
            val fieldAnn = i.asTerm.annotations.find(_.tree.tpe =:= ru.typeOf[DBFFieldProp])
            val ann = if (fieldAnn.isDefined) {
              Some(getAnnotationData(fieldAnn.get.tree).name)
            } else {
              logWarning(s"类 ${clazz.getName} 的字段 ${i.name} 未定义注解!")
              None
            }
            Tuple2(i.asTerm, ann)
          })

        var dbfRow: DBFRow = null

        breakable {
          while (true) {
            dbfRow = reader.nextRow()
            if (dbfRow == null) {
              break()
            }
            val data = conv(reader.getCurrentOffset, reader.getFields, dbfRow, param, runtimeMirror, classMirror, constructorMethod, reflectFields)
            result += (data)
          }
        }

      }
      logInfo(s"分区${partition.idx} 读取文件第 ${startOffset} 条至 ${endOffset} 条记录，共读取 ${result.size} 条数据！") //读取记录数小于计划读取数：DBF 中有记录标记未删除
      result.iterator
    } finally {
      IOUtils.closeQuietly(reader)
      IOUtils.closeQuietly(inputStream)
      IOUtils.closeQuietly(fs)
    }


  }


  override protected def getPartitions: Array[Partition] = partitions
}


object DBFReaderRDD {
  def load(): Unit = {

  }


  def calcOffset(recordCount: Int,
                 idx: Int,
                 partitionNum: Int): (Int, Int) = {
    val ptnRecordCount = recordCount / partitionNum
    val mode = recordCount % partitionNum
    val (offset, stepSize) = if (ptnRecordCount == 0) {
      if (idx < mode) {
        (idx, 1)
      } else {
        (mode, 0)
      }
    } else {
      if (idx < mode) {
        (idx * (ptnRecordCount + 1), (ptnRecordCount + 1))
      } else {
        (mode * (ptnRecordCount + 1) + (idx - mode) * ptnRecordCount, ptnRecordCount)
      }

    }
    (offset, offset + stepSize)
  }


}