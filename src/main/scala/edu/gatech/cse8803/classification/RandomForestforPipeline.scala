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

package org.apache.spark.ml.classification

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.param._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.{VectorUDT, BLAS, Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.model._



private[ml] trait HasStrategy extends Params {
  /**
   * param for Strategy
   * @group param
   */
  val strategy: Param[String] =
    new Param(this, "strategy", "Strategy name", Some("Classification"))

  /** @group getParam */
  def getStrategy: String = get(strategy)
}

private[ml] trait HasNumTrees extends Params {
  /**
   * param for number of trees
   * @group param
   */
  val numTrees: IntParam = new IntParam(this, "numTrees", "number of trees")

  /** @group getParam */
  def getNumTrees: Int = get(numTrees)
}

private[ml] trait HasFeatureSubsetStrategy extends Params {
  /**
   * param for Feature Subset Strategy
   * @group param
   */
  val featureSubsetStrategy: Param[String] =
    new Param(this, "featureSubsetStrategy", "Feature Subset Strategy name", Some("auto"))

  /** @group getParam */
  def getFeatureSubsetStrategy: String = get(featureSubsetStrategy)
}


//private[ml] trait HasSeed extends Params {
//  /**
//   * param for seed
//   * @group param
//   */
//  val seed: IntParam = new IntParam(this, "seed", "seed")
//
//  /** @group getParam */
//  def getSeed: Int = get(seed)
//}



/**
 * Params for RandomForest
 */
private[classification] trait RandomForestParams extends ProbabilisticClassifierParams
  with HasStrategy with HasNumTrees with HasFeatureSubsetStrategy //with HasSeed


/**
 * :: AlphaComponent ::
 *
 * RandomForest
 * Currently, this class only supports binary classification.
 */
@AlphaComponent
class RandomForestForPipeline
  extends ProbabilisticClassifier[Vector, RandomForestForPipeline, RandomForestModelForPipeline]
  with RandomForestParams {

  setStrategy("Classification")
  setNumTrees(3)
  setFeatureSubsetStrategy("auto")
  //setSeed(System.currentTimeMillis().toInt)

  /** @group setParam */
  def setStrategy(value: String): this.type = set(strategy, value)

  /** @group setParam */
  def setNumTrees(value: Int): this.type = set(numTrees, value)

  /** @group setParam */
  def setFeatureSubsetStrategy(value: String): this.type = set(featureSubsetStrategy, value)
  
  ///** @group setParam */
  //def setSeed(value: Int): this.type = set(seed, value)


  override protected def train(dataset: DataFrame, paramMap: ParamMap): RandomForestModelForPipeline = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val oldDataset = extractLabeledPoints(dataset, paramMap)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }
	//var weightsBag : RDD[Vector] = null;
	//var weightsBag:Array[Vector] = new Array[Vector](paramMap(bagSize))
	//var interceptBag:Array[Double] = new Array[Double](paramMap(bagSize))
	
	println("Dataset size: " + oldDataset.count.toString)
	
	val treeStrategy = Strategy.defaultStrategy(paramMap(strategy))
	val numOfTrees = paramMap(numTrees)
	val featSubsetStrategy = paramMap(featureSubsetStrategy)
	//val randSeed = paramMap(seed)
		
	//println("Random forest with 3 trees")
	val modelRF = RandomForest.trainClassifier(oldDataset, treeStrategy, numOfTrees, featSubsetStrategy, System.currentTimeMillis().toInt)
		
	val rm = new RandomForestModelForPipeline(this, paramMap, modelRF)
	
    if (handlePersistence) {
      oldDataset.unpersist()
    }
    rm
  }
}


/**
 * :: AlphaComponent ::
 *
 * Model produced by [[LogisticRegression]].
 */
@AlphaComponent
class RandomForestModelForPipeline private[ml] (
    override val parent: RandomForestForPipeline,
    override val fittingParamMap: ParamMap,
    val rfModel: RandomForestModel)
  extends ProbabilisticClassificationModel[Vector, RandomForestModelForPipeline]
  with RandomForestParams {

  
  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    // This is overridden (a) to be more efficient (avoiding re-computing values when creating
    // multiple output columns) and (b) to handle threshold, which the abstractions do not use.
    // TODO: We should abstract away the steps defined by UDFs below so that the abstractions
    // can call whichever UDFs are needed to create the output columns.

    // Check schema
    transformSchema(dataset.schema, paramMap, logging = true)

    val map = this.paramMap ++ paramMap

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    //   rawPrediction (-margin, margin)
    //   probability (1.0-score, score)
    //   prediction (max margin)
    var tmpData = dataset
    var numColsOutput = 0
    if (map(rawPredictionCol) != "") {
      val features2raw: Vector => Vector = (features) => predictRaw(features)
      tmpData = tmpData.withColumn(map(rawPredictionCol),
        callUDF(features2raw, new VectorUDT, col(map(featuresCol))))
      numColsOutput += 1
    }
    if (map(probabilityCol) != "") {
      if (map(rawPredictionCol) != "") {
        val raw2prob = udf { (rawPreds: Vector) =>
          Vectors.dense(1.0 - rawPreds(1), rawPreds(1)): Vector
        }
        tmpData = tmpData.withColumn(map(probabilityCol), raw2prob(col(map(rawPredictionCol))))
      } else {
        val features2prob = udf { (features: Vector) => predictProbabilities(features) : Vector }
        tmpData = tmpData.withColumn(map(probabilityCol), features2prob(col(map(featuresCol))))
      }
      numColsOutput += 1
    }
    if (map(predictionCol) != "") {
      if (map(probabilityCol) != "") {
        val predict = udf { probs: Vector =>
          probs(1)
        }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(probabilityCol))))
      } else if (map(rawPredictionCol) != "") {
        val predict = udf { rawPreds: Vector =>
         rawPreds(1)
        }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(rawPredictionCol))))
      } else {
        val predict = udf { features: Vector => this.predict(features) }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(featuresCol))))
      }
      numColsOutput += 1
    }
    if (numColsOutput == 0) {
      this.logWarning(s"$uid: RandomForestModelForPipeline.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    tmpData
  }

  
  override val numClasses: Int = 2

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[threshold]].
   */
  override protected def predict(features: Vector): Double = {
     rfModel.predict(features)
  }

  override protected def predictProbabilities(features: Vector): Vector = {
    Vectors.dense(1.0 - rfModel.predict(features), rfModel.predict(features)) 
  }

  override protected def predictRaw(features: Vector): Vector = {
    Vectors.dense(1.0 - rfModel.predict(features), rfModel.predict(features)) 
  }
  
  override protected def copy(): RandomForestModelForPipeline = {
    val m = new RandomForestModelForPipeline(parent, fittingParamMap, rfModel)
    Params.inheritValues(this.paramMap, this, m)
    m
  }
}
