package com.linuxense.javadbf.spark

import java.sql.Date

import com.linuxense.javadbf.spark.Utils._
import com.linuxense.javadbf.{DBFDataType, DBFField, DBFFieldNotFoundException, DBFRow}
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

sealed trait DBFDataHandler[V <: DBFParam, F <: DBFTransferParam] extends Serializable with org.apache.spark.internal.Logging {

  def buildParam(clazz: Class[_]): F

  def adjustFields(fields: Array[DBFField]): Unit = {

  }

  def transfer[T](offset: Int,
                  fields: Array[DBFField],
                  data: DBFRow,
                  dBFOptParam: List[V],
                  transferParam: F): T


}

case class DBFRowHandler() extends DBFDataHandler[DBFOptParam, DBFRowTransferParam] {

  override def buildParam(clazz: Class[_]): DBFRowTransferParam = {
    null
  }

  override def transfer[T](offset: Int,
                           fields: Array[DBFField],
                           data: DBFRow,
                           dBFOptParam: List[DBFOptParam],
                           transferParam: DBFRowTransferParam): T = {
    val arrayBuffer = ArrayBuffer[Any](offset)

    dBFOptParam.sortBy(_.orderSn).foreach(i => {
      arrayBuffer += i.value
    })

    for (idx <- 0 until (fields.length)) {
      val oldData = data.getObject(idx)
      val v = fields(idx).getType match {
        case DBFDataType.DATE | DBFDataType.TIMESTAMP if (oldData != null) =>
          new Date(oldData.asInstanceOf[java.util.Date].getTime)
        case _ => oldData
      }

      arrayBuffer += v
    }
    val row = Row.fromSeq(arrayBuffer)
    row.asInstanceOf[T]
  }
}

case class DBFRowOrderHandler() extends DBFDataHandler[DBFOptParam, DBFRowTransferParam] {

  override def buildParam(clazz:Class[_]): DBFRowTransferParam = {
    null
  }


  override def transfer[T](offset: Int,
                           fields: Array[DBFField],
                           data: DBFRow,
                           dBFOptParam: List[DBFOptParam],
                           tranferParam: DBFRowTransferParam): T = {
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

case class DBFBeanHandler() extends DBFDataHandler[DBFOptDFParam, DBFBeanTransferParam] {

  override def buildParam(clazz:Class[_]): DBFBeanTransferParam = {

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
        val fieldAnn = i.asTerm.annotations.find(_.tree.tpe =:= ru.typeOf[Column])
        val ann = if (fieldAnn.isDefined) {
          Some(getAnnotationData(fieldAnn.get.tree).name)
        } else {
          logWarning(s"类 ${clazz.getName} 的字段 ${i.name} 未定义注解!")
          None
        }
        Tuple2(i.asTerm, ann)
      })
    DBFBeanTransferParam(runtimeMirror,classMirror,constructorMethod,reflectFields)
  }

  override def transfer[T](offset: Int,
                           fields: Array[DBFField],
                           data: DBFRow,
                           dBFOptParam: List[DBFOptDFParam],
                           transferParam:DBFBeanTransferParam
                          ): T = {

    val instance = transferParam.constructor()
    val ref = transferParam.runTimeMirror.reflect(instance)
    transferParam.classFields.foreach(i => {
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
      transferParam.classFields.find(f => f._1.name.decodedName.toString.trim == i.name) match {
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



