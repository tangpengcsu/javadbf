package com.linuxense.javadbf.spark

import com.linuxense.javadbf.{DBFField, DBFRow}
import scala.reflect.runtime.universe._

sealed trait DBFTransferParam {

}

case class DBFRowTransferParam() extends DBFTransferParam

case class DBFBeanTransferParam(runTimeMirror: RuntimeMirror,
                               classMirror: ClassMirror,
                               constructor: MethodMirror,
                               classFields: Iterable[Tuple2[TermSymbol, Option[String]]]
                              ) extends DBFTransferParam
