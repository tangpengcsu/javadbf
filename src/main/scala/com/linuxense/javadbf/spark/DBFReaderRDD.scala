package com.linuxense.javadbf.spark

import java.nio.charset.Charset

import com.linuxense.javadbf.spark.Utils._
import com.linuxense.javadbf.{DBFField, DBFRow, DBFSkipRow}
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

class DBFReaderRDD[T: ClassTag, V <: DBFParam, F <: DBFTransferParam, H <: DBFDataHandler[V, F]](sparkContext: SparkContext,
                                                                                                 path: String,
                                                                                                 handler: H,
                                                                                                 charSet: String,
                                                                                                 showDeletedRows: Boolean,
                                                                                                 userName: String,
                                                                                                 connectTimeout: Int,
                                                                                                 maxRetries: Int,
                                                                                                 partitions: Array[Partition],
                                                                                                 param: List[V] = Nil,
                                                                                                 clazz: Class[_])
  extends RDD[T](sparkContext, deps = Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition = split.asInstanceOf[DBFPartition]

    val dbfHelper = DBFHelper(path, Charset.forName(charSet), userName, showDeletedRows, connectTimeout, maxRetries)
    try {
      dbfHelper.open()
      val reader = dbfHelper.getReader
      handler.adjustFields(reader.getFields)//调整字段长度

      val recoderCount = reader.getRecordCount

      val result: mutable.ListBuffer[T] = ListBuffer()
      val (startOffset, endOffset) = DBFReaderRDD.calcOffset(recoderCount, partition.idx, partitions.size)


      if (recoderCount != 0 && startOffset != endOffset) {
        reader.setStartOffset(startOffset)
        reader.partitionIdx = partition.idx
        reader.setEndOffset(endOffset)
        val transferParam = handler.buildParam(clazz)
        var dbfRow: DBFRow = null

        breakable {
          while (true) {
            dbfRow = reader.nextRow()
            if (dbfRow == null) {
              break()
            }
            dbfRow match {
              case e: DBFSkipRow =>
              case _ =>
                val data = handler.transfer[T](reader.getCurrentOffset, reader.getFields, dbfRow, param,transferParam)
                result += (data)
            }

          }
        }

      }
      logInfo(s"分区${partition.idx} 读取文件第 ${startOffset} 条至 ${endOffset} 条记录，共读取 ${result.size} 条数据！") //读取记录数小于计划读取数：DBF 中有记录标记未删除
      result.iterator
    } finally {
      dbfHelper.close()
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