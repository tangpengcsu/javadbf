package com.linuxense.javadbf.spark

import com.linuxense.javadbf.spark.Utils._
import com.linuxense.javadbf.{DBFField, DBFFieldNotFoundException, DBFRow}
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

sealed trait DBFDataHandler[V <: DBFParam] extends Serializable with org.apache.spark.internal.Logging {
  def process[T](offset: Int,
                 fields: Array[DBFField],
                 data: DBFRow,
                 dBFOptParam: List[V],
                 runTimeMirror: ru.RuntimeMirror,
                 classMirror: ru.ClassMirror,
                 constructor: ru.MethodMirror,
                 classFields: Iterable[Tuple2[ru.TermSymbol, Option[String]]]
                ): T
}

object DBFRowHandler extends DBFDataHandler[DBFOptParam] {
  override def process[T](offset: Int, fields: Array[DBFField], data: DBFRow, dBFOptParam: List[DBFOptParam], runTimeMirror: ru.RuntimeMirror, classMirror: ru.ClassMirror, constructor: ru.MethodMirror, classFields: Iterable[Tuple2[ru.TermSymbol, Option[String]]]): T = {
    val arrayBuffer = ArrayBuffer[Any](offset)

    dBFOptParam.sortBy(_.orderSn).foreach(i => {
      arrayBuffer += i.value
    })
    for (idx <- 0 until (fields.length)) {
      arrayBuffer += data.getObject(idx)
    }

    val row = Row.fromSeq(arrayBuffer)
    row.asInstanceOf[T]
  }
}
object DBFRowOrderHandler extends DBFDataHandler[DBFOptParam] {
  override def process[T](offset: Int, fields: Array[DBFField],
                          data: DBFRow, dBFOptParam: List[DBFOptParam],
                          runTimeMirror: ru.RuntimeMirror,
                          classMirror: ru.ClassMirror,
                          constructor: ru.MethodMirror,
                          classFields: Iterable[Tuple2[ru.TermSymbol, Option[String]]]): T = {
    val arrayBuffer = ArrayBuffer[Any](offset)

    dBFOptParam.sortBy(_.orderSn).foreach(i => {
      arrayBuffer += i.value
    })
    for (idx <- 0 until (fields.length)) {
      arrayBuffer += data.getObject(idx)
    }

    val row = Row.fromSeq(arrayBuffer)
    row.asInstanceOf[T]
  }
}
object DBFBeanHandler extends DBFDataHandler[DBFOptDFParam] {
  override def process[T](offset: Int,
                          fields: Array[DBFField],
                          data: DBFRow,
                          dBFOptParam: List[DBFOptDFParam],
                          runTimeMirror: ru.RuntimeMirror,
                          classMirror: ru.ClassMirror,
                          constructor: ru.MethodMirror,
                          classFields: Iterable[Tuple2[ru.TermSymbol, Option[String]]]): T = {

    val instance = constructor()
    val ref = runTimeMirror.reflect(instance)
    classFields.foreach(i => {
        try {
          val fm = ref.reflectField(i._1)
          //先取注解ming，后取字段名
          val fieldName = i._2 match {
            case Some(s: String) => s
            case None => fm.symbol.name.decodedName.toString.trim
          }
          val d = transData(fieldName, fm.symbol.typeSignature.typeSymbol.name.decodedName.toString, data)
          fm.set(d)
        } catch {
          case e: DBFFieldNotFoundException =>
            logDebug(s"${ref.symbol.name} 类定义字段未在 dbf 中：${i._1.name.toString.trim}")
        }
      })
    dBFOptParam.foreach(i => {
      classFields.find(f => f._1.name.decodedName.toString.trim == i.name) match {
        case Some(ff) =>
          val fm = ref.reflectField(ff._1)
          val v = if (i.isCounter) {
            offset
          } else {
            i.value
          }
          fm.set(v)
        case None => logWarning(s"自定义字段${i.name}未定义在 ${ref.symbol.name} 类中！")
      }


    })
    instance.asInstanceOf[T]
  }

}



