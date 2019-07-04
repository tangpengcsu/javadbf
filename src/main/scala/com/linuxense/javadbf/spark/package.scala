package com.linuxense.javadbf

import org.apache.spark.SparkContext

package object spark {
  implicit def toSparkContextFunctions(sc: SparkContext):DBFFunctions  = {

    new DBFFunctions(sc)
  }
}
