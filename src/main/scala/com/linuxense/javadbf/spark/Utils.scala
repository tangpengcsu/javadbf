package com.linuxense.javadbf.spark

import com.linuxense.javadbf.{DBFDataType, DBFRow}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object Utils {

  def getClazz[T]()(implicit m: ClassTag[T]): Class[T] = {
    m.runtimeClass.asInstanceOf[Class[T]]
  }

  def transData(fieldName:String,dataType:String,data: DBFRow): Any ={

    dataType match {
      case "String"=> data.getString(fieldName)
      case "BigDecimal"=> BigDecimal(data.getBigDecimal(fieldName))//只能反射 scala.math.BigDecimal，不能反射 java.math.BigDecimal
      case "Int"|"Integer"=>data.getInt(fieldName)
      case "Short"=>data.getInt(fieldName).toShort
      case "Long"=>data.getLong(fieldName)
      case "Float"=>data.getFloat(fieldName)
      case "Double"=>data.getDouble(fieldName)
      case "Char"|"Character"=> data.getString(fieldName).charAt(0)
      case _=> throw new IllegalArgumentException(s"不支持改反射数据类型：${fieldName},${dataType}")

    }
  }

  def tranClassData(dataType:Class[_],value:Any): Unit ={
    dataType.getSimpleName match {
      case "String"=>
      case "BigDecimal"=>
      case "int"|"Integer"=>
      case "short"|"Short"=>
      case "long"|"Long"=>
      case "float"|"Float"=>
      case "double"|"Double"=>
      case _=>throw new IllegalArgumentException(s"不支持改反射数据类型：${dataType}")
    }

  }

import com.linuxense.javadbf.DBFDataType._
  def dataType(typeName: DBFDataType, scale:Int): DataType = typeName match {
    /*https://github.com/tangpengcsu/javadbf*/
    case LONG|AUTOINCREMENT ⇒ IntegerType
    case FLOATING_POINT ⇒ FloatType
    case DOUBLE ⇒ DoubleType
    case NUMERIC|CURRENCY ⇒ DecimalType(DecimalType.MAX_PRECISION, scale)
    case VARCHAR |CHARACTER ⇒ StringType
    case DATE | TIMESTAMP ⇒ DateType
    case BINARY|VARBINARY|PICTURE ⇒ BinaryType
    case _ ⇒ StructType(new Array[StructField](0))
  }
  //获取dbf字段注解
  def getAnnotationData(tree: Tree) = {
    val Apply(_, Literal(Constant(name: String)) :: Nil) = tree
    new Column(name)
}
  // 获取指定类型的注解信息，通过 Annotation.tree.tpe 获取注解的 Type 类型，以此进行筛选
  def getClassAnnotation[T: TypeTag, U: TypeTag] =
    symbolOf[T].annotations.find(_.tree.tpe =:= typeOf[U])

  // 通过字段名称获取指定类型的注解信息，注意查找字段名称时添加空格
  def getMemberAnnotation[T: TypeTag, U: TypeTag](memberName: String) =
    typeOf[T].decl(TermName(s"$memberName ")).annotations.find(_.tree.tpe =:= typeOf[U])

  // 通过方法名称和参数名称获取指定类型的注解信息
  def getArgAnnotation[T: TypeTag, U: TypeTag](methodName: String, argName: String) =
    typeOf[T].decl(TermName(methodName)).asMethod.paramLists.collect {
      case symbols => symbols.find(_.name == TermName(argName))
    }.headOption.fold(Option[Annotation](null))(_.get.annotations.find(_.tree.tpe =:= typeOf[U]))


}
