package com.linuxense.javadbf.spark

import scala.beans.BeanProperty

class file extends Serializable{
  var settDate: Int = _
  var settBatNo: Int = _
  var sn:Long=_
  @Column("asset_d003")
  var ASSET_D003: BigDecimal = BigDecimal(0, 2)
}

class RjbBean extends file {

  @BeanProperty
  @Column("item_le000")
  var item_le000: String = ""

  @Column("asset_l001")
  var asset_l001: BigDecimal = BigDecimal(0, 2)
  // @DBFFieldProp("incomepay")
  var incomepay: BigDecimal = BigDecimal(0, 2)
  @Column("item_de002")
  val ITEM_DE002: String="1"



  override def toString = s"RjbBean(settDate=$settDate, settBatNo=$settBatNo, sn=$sn, item_le000=$item_le000, ASSET_D003=$ASSET_D003, asset_l001=$asset_l001, incomepay=$incomepay, ITEM_DE002=$ITEM_DE002)"
}
