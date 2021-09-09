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

package org.apache.spark.ml.instance

import scala.util.Random
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.annotation.Since
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Params, IntParam, BooleanParam, ParamValidators, ParamMap}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasFeaturesCol, HasSeed}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable, MetadataUtils}
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.DoubleParam
import org.apache.spark.ml.param.IntArrayParam
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * LSH-IS algorithms.
  * Instance selection algorithms for Big Data based on locality sensitive 
  * hashing.
  * Two algorithms are implemented:
  * - LSH-IS-S: One instance of each class per bucket is selected.
  * - LSH-IS-F (filter): If there is only one instance of one class into the 
  *   bucket, it is removed.
  * 
  * @author Álvar Arnaiz-González <alvarag@ubu.es>
  * @param uid
  */
@Since("2.4.5")
class LSHIS @Since("2.4.5") (@Since("2.4.5") override val uid: String) 
  extends Transformer with LSHISParams with HasLabelCol with HasFeaturesCol 
  with HasSeed with DefaultParamsWritable {

  @Since("2.4.5")
  def this() = this(Identifiable.randomUID("lshis"))

  /** @group setParam */
  @Since("2.4.5")
  def setBucketLength(value: Double): this.type = set(bucketLength, value)

  /** @group setParam */
  @Since("2.4.5")
  def setNumHashTablesAnd(value: Int): this.type = set(numHashTablesAnd, value)

  /** @group setParam */
  @Since("2.4.5")
  def setNumHashTablesOr(value: Int): this.type = set(numHashTablesOr, value)

  /** @group setParam */
  @Since("2.4.5")
  def setFilter(value: Boolean): this.type = set(filter, value)

  /** @group setParam */
  @Since("2.4.5")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
    * Performs the instance selectionTransforms the dataset into an oversampled one.
    *
    * @param ds dataset.
    * 
    * @return an oversampled DataFrame.
    */
  override def transform(ds: Dataset[_]): DataFrame = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(ds.schema($(featuresCol)))

    require(categoricalFeatures.isEmpty, "LSHIS requires all features to be numeric.")
    
    val session = ds.sparkSession
    val context = session.sparkContext
    val rnd = new Random($(seed))
    var rddSelected = new Array[RDD[Tuple2[Double, Vector]]]($(numHashTablesOr))

    // OR-construction
    for (i <- 0 to $(numHashTablesOr) - 1) {
      val model = new BucketedRandomProjectionLSH()
        .setBucketLength($(bucketLength))
        .setNumHashTables($(numHashTablesAnd))
        .setInputCol($(featuresCol))
        .setSeed(rnd.nextLong)
        .setOutputCol("hashes")
        .fit(ds)
      
      // Compute hashes and group instances by them (AND-construction)
      rddSelected(i) = model
        .transform(ds)
        .select("hashes", $(labelCol), $(featuresCol))
        .rdd
        .groupBy(_(0))
        .flatMapValues(if($(filter)) instanceSelectionFilter else instanceSelectionOnePerBucket)
        .map(_._2)
    }

    // Union of the selected instances
    val unionSelected = context.union(rddSelected).distinct

    // Create a DF with the selected instances
    session.createDataFrame(unionSelected).toDF($(labelCol), $(featuresCol))
  }

  private def instanceSelectionOnePerBucket(hashInstances: Iterable[Row]): 
      List[Tuple2[Double, Vector]] = {
    var instsSelected = new ArrayBuffer[Tuple2[Double, Vector]]
    var countInsts = HashMap.empty[Double, Int]

    // Select one instances of each class per bucket
    for (inst <- hashInstances) {
      // inst(0) contains the hash
      val label = inst(1).asInstanceOf[Double]
      val features = inst(2).asInstanceOf[Vector]

      if (!countInsts.contains(label)) {
        countInsts += (label -> 1)
        instsSelected.append((label, features))
      }
    }

    instsSelected.toList
  }

private def instanceSelectionFilter(hashInstances: Iterable[Row]): 
      List[Tuple2[Double, Vector]] = {
    var instsSelected = new ArrayBuffer[Tuple2[Double, Vector]]
    var countInsts = HashMap.empty[Double, Int]
    var candidatesPerClass = HashMap.empty[Double, Vector]

    // Compute the number of instances per class per bucket
    for (inst <- hashInstances) {
      // inst(0) contains the hash
      val label = inst(1).asInstanceOf[Double]
      val features = inst(2).asInstanceOf[Vector]

      if (!countInsts.contains(label))
        candidatesPerClass += ((label, features))

      countInsts += (label -> 1)
    }

    // Select an instance if there is more than one of the same class
    for ((lbl, count) <- countInsts) {
      if (count > 1) {
        val features = candidatesPerClass.get(lbl).get
        instsSelected.append((lbl, features))
      }
    }

    instsSelected.toList
  }

  /**
   * The schema of the output Dataset is the same as the input one.
   * 
   * @param schema Input schema.
   */
  @Since("2.4.5")
  override def transformSchema(schema: StructType): StructType = schema

  /**
   * Creates a copy of this instance.
   * 
   * @param extra  Param values which will overwrite Params in the copy.
   */
  @Since("2.4.5")
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

/**
  * Parameters of LSH-IS algorithm.
  * - bucketLength: Lenght of the buckets.
  * - numHashTablesAnd: The number of hash tables for AND-construction.
  * - numHashTablesOr: The number of hash tables for OR-construction.
  * - filter: Whether filter or only select one instance per class per bucket.
  * 
  * @author Álvar Arnaiz-González <alvarag@ubu.es>
  */
@Since("2.4.5")
trait LSHISParams extends Params {

  /** @group param */
  @Since("2.4.5")
  final val bucketLength: DoubleParam = new DoubleParam(this, "bucketLength", 
    "Length of the buckets.",
    ParamValidators.gt(0.0))

  /** @group param */
  @Since("2.4.5")
  final val numHashTablesAnd: IntParam = new IntParam(this, "numHashTablesAnd",
    "Number of hash tables for AND-construction.", ParamValidators.gtEq(1))

  /** @group param */
  @Since("2.4.5")
  final val numHashTablesOr: IntParam = new IntParam(this, "numHashTablesOr",
    "Number of hash tables for OR-construction.", ParamValidators.gtEq(1))

  /** @group param */
  @Since("2.4.5")
  final val filter: BooleanParam = new BooleanParam(this, "filter",
    "Whether filter or select one instance per class per bucket.")

  setDefault(bucketLength -> 1.0, numHashTablesAnd -> 10, numHashTablesOr -> 4, filter -> true)

  /** @group getParam */
  @Since("2.4.5")
  final def getBucketLength: Double = $(bucketLength)
  
  /** @group getParam */
  @Since("2.4.5")
  final def getNumHashTablesAnd: Int = $(numHashTablesAnd)

  /** @group getParam */
  @Since("2.4.5")
  final def getNumHashTablesOr: Int = $(numHashTablesOr)

  /** @group getParam */
  @Since("2.4.5")
  final def getFilter: Boolean = $(filter)
}
