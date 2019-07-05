package com.linuxense.javadbf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

package object spark {
  implicit def toSparkContextFunctions(sparkSession:SparkSession):DBFFunctions  = {

    new DBFFunctions(sparkSession)
  }
}
