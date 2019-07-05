package com.linuxense.javadbf.spark

import scala.beans.BeanProperty

class CaseFile() extends Serializable {
  val settDate: Int = 1
  val settBatNo: Int = 1
  val sn: Long = 1


  override def toString = s"CaseFile(settDate=$settDate, settBatNo=$settBatNo, sn=$sn)"
}

case class CaseRjbBean(

                        @DBFFieldProp(name="asset_d003")
                        var ASSET_D003: BigDecimal, @BeanProperty
                        @DBFFieldProp("item_le000")
                        var item_le000: String,
                        @DBFFieldProp("asset_l001")
                        var asset_l001: BigDecimal,
                        var incomepay: BigDecimal, @DBFFieldProp("item_de002")
                        val ITEM_DE002: String) extends CaseFile() {

  override val sn:Long =1
  def this() = this(null, null, null, null, null)

  override def toString = s"CaseRjbBean(settDate=$settDate, settBatNo=$settBatNo, sn=$sn, item_le000=$item_le000, ASSET_D003=$ASSET_D003, asset_l001=$asset_l001, incomepay=$incomepay, ITEM_DE002=$ITEM_DE002)"
}
