package org.ubu.admirable.util

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.{ VectorAssembler, StringIndexer, OneHotEncoder }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType

object LibSVMReader {
  
  def libSVMToML(path: String, session: SparkSession): Dataset[_] = {
    
    val libsvmData = session
      .read
      .format("libsvm")
      .option("inferSchema", "true")
      .load(path)

    new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(libsvmData)
      .transform(libsvmData)
      .drop("label")
      .withColumnRenamed("indexedLabel", "label")
  }
  
}