package com.linuxense.javadbf.spark

class Fruit(  f12:String) extends Serializable {
  var f1:String=_
  val f2:String ="-1"

  override def toString = s"Fruit(f1=$f1, f2=$f2)"
}

class V1(  val f12: String,
         override val f2: String) extends Fruit(f12){
  var v1:String =_
  val v2:String = "-1"

  def this()= this(null,null)


  override def toString = s"V1($v1, $v2,${f1},${f2},${f12})"
}
