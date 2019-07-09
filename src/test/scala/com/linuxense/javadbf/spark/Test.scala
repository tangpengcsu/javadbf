package com.linuxense.javadbf.spark

import org.scalatest.FunSuite

class Test extends FunSuite {

  test("fold") {
      val ls = List(1,2,3,4)
     val z=ls.foldLeft("-")((i,j)=>i+j)
    println(z)
  }
}
