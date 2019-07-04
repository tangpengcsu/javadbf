package com.linuxense.javadbf.spark

class Fruit extends Serializable {
  var f1:String=_
  val f2:String ="-1"
}

class V1 extends Fruit{
  var v1:String =_
  val v2:String = "-1"


  override def toString = s"V1($v1, $v2,${f1},${f2})"
}
