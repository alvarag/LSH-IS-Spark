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
import scala.util.Random
import org.ubu.admirable.util.LibSVMReader
import org.apache.spark.ml.instance.LSHIS
import org.apache.spark.ml.classification.DecisionTreeClassifier
import java.time.Instant
import java.time.Duration

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
        (0 to res.length - 1).map{ i =>
          res(i).union(part(i))
        }.toArray
      }
    }.toArray
  }

  def trainTestSplits(repsAndFolds: Array[Array[Dataset[Row]]]): Array[Array[(Dataset[Row], Dataset[Row])]] = {
    repsAndFolds.map { folds =>
      val ids = (0 until folds.length).toArray
      ids.map { testId =>
        val idsBuffer = ids.toBuffer
        idsBuffer.remove(testId)
        val trainFold = idsBuffer.map{ trainId =>
          folds(trainId)
        }.reduce{ (res, part) =>
          res.union(part)
        }
        val testFold = folds(testId)
        (trainFold, testFold)
      }
    }
  }

  def main(args: Array[String]) {
    val session = SparkSession.builder.appName("LSH-IS-Spark experiment launcher")
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    val rnd = new Random(46)

    val inputDataset = args(0)
    val bucketLength = args(1).toInt
    val numHashTablesAndArr = args(2)
      .split(",")
      .map(_.trim)
      .filter(_.length > 0)
      .map(_.toInt).toArray
    val numHashTablesOrArr = args(3)
      .split(",")
      .map(_.trim)
      .filter(_.length > 0)
      .map(_.toInt).toArray
    val cvReps = args(4).toInt
    val cvFolds = args(5).toInt
    val outputPath = args(6)

    val dataset = LibSVMReader.libSVMToML(inputDataset, session)
      .select(col("label"), col("features")).cache()

    val repsFolds = makeFolds(dataset, cvReps, cvFolds, rnd.nextLong)
    val repsTrainTestFolds = trainTestSplits(repsFolds)

    val reduceResults = (res: (Dataset[Row], IndexedSeq[Dataset[Row]]), 
        part: (Dataset[Row], IndexedSeq[Dataset[Row]])) => {
      val resBase = res._1
      val resLSH = res._2
      val partBase = part._1
      val partLSH = part._2

      val unionResLSH = (0 until partLSH.length).map{ i =>
        resLSH(i).union(partLSH(i))
      }

      (resBase.union(partBase), unionResLSH)
    }

    val (resBase, resLSH) = repsTrainTestFolds.zipWithIndex.map{ case (folds, nrep) =>
      folds.zipWithIndex.map { case ((trainFold, testFold), nfold) =>
        
        val cachedTrainFold = trainFold.cache
        val originalSize = cachedTrainFold.count
        val cachedTestFold = testFold.cache
        cachedTestFold.count // force cache

        // training models
        val dt = new DecisionTreeClassifier().setSeed(rnd.nextLong)
        val baseDTModel = dt.fit(cachedTrainFold)
        val lshModelsAndSizes = (0 until numHashTablesOrArr.length).map{ i =>
          val lshis = new LSHIS()
            .setBucketLength(bucketLength)
            .setNumHashTablesAnd(numHashTablesAndArr(i))
            .setNumHashTablesOr(numHashTablesOrArr(i))
            .setSeed(rnd.nextLong)

            val init = Instant.now
            val filteredTrainFold = lshis.transform(cachedTrainFold).cache
            val filteredSize = filteredTrainFold.count
            val stop = Instant.now
            val milis = Duration.between(init, stop).toMillis
            val ret = (dt.fit(filteredTrainFold), filteredSize, milis)
            filteredTrainFold.unpersist(true)
            ret
        }

        // predictions
        val baseResults = baseDTModel
          .transform(cachedTestFold)
          .select(col("label"), col("prediction"))
          .withColumnRenamed("label", "true")
          .withColumnRenamed("prediction", "predicted")
          .withColumn("repetition", lit(nrep+1))
          .withColumn("fold", lit(nfold+1))
          .withColumn("trainSize", lit(originalSize)).cache
        val lshResults = lshModelsAndSizes.map{ case(model, trainSize, milis) =>
          model
            .transform(cachedTestFold)
            .select(col("label"), col("prediction"))
            .withColumnRenamed("label", "true")
            .withColumnRenamed("prediction", "predicted")
            .withColumn("repetition", lit(nrep+1))
            .withColumn("fold", lit(nfold+1))
            .withColumn("trainSize", lit(trainSize))
            .withColumn("time", lit(milis)).cache
        }
        cachedTrainFold.unpersist(true)
        cachedTestFold.unpersist(true)

        (baseResults, lshResults)
      }.reduce(reduceResults)
    }.reduce(reduceResults)

    resBase.write.csv(outputPath + "/base")
    (0 until resLSH.length).map{ i =>
      resLSH(i).write.csv(outputPath + "/lsh_and"+numHashTablesAndArr(i)+"_or"+numHashTablesOrArr(i))
    }
    
  }

}