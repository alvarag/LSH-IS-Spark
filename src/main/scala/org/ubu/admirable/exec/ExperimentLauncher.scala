/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ubu.admirable.exec

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExperimentLauncher {

  def splitDataset(ds: Dataset[Row], folds: Int, seed: Long): Array[Dataset[Row]] = {
    val weight = 1.0 / folds.toDouble
    ds.randomSplit((1 to folds).map{_ => weight}.toArray, seed)
  }

  def makeFolds(ds: Dataset[Row], reps: Int, folds: Int, seed: Long): Array[Array[Dataset[Row]]] = {
    val labels = ds.groupBy("label")
      .count
      .collect
      .map{r => r.getAs[Double](0)}

    val dsSplits = labels.map{l => ds.filter(col("label") === l)} // one dataset per label
    
    // data set repetitions and stratified folds
    (1 to reps).map{ rep =>
      dsSplits.map{ split =>
        splitDataset(split, folds, seed)
      }.reduce{ (res, part) =>
        (1 to res.length).map{ i =>
          res(i).union(part(i))
        }.toArray
      }
    }.toArray
  }

  def main(args: Array[String]) {
    val session = SparkSession.builder.appName("LSH-IS-Spark experiment launcher")
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
  }

}