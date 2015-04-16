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


private[ml] trait HasBagSize extends Params {
  /**
   * param for number of models in bag
   * @group param
   */
  val bagSize: IntParam = new IntParam(this, "bagSize", "number of bags in model")

  /** @group getParam */
  def getBagSize: Int = get(bagSize)
}






/**
 * Params for logistic regression.
 */
private[classification] trait BaggedLogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasMaxIter with HasThreshold with HasBagSize


/**
 * :: AlphaComponent ::
 *
 * Logistic regression.
 * Currently, this class only supports binary classification.
 */
@AlphaComponent
class BaggedLogisticRegression
  extends ProbabilisticClassifier[Vector, BaggedLogisticRegression, BaggedLogisticRegressionModel]
  with BaggedLogisticRegressionParams {

  setRegParam(0.1)
  setMaxIter(100)
  setThreshold(0.5)
  setBagSize(10)

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)
  
  /** @group setParam */
  def setBagSize(value: Int): this.type = set(bagSize, value)


  override protected def train(dataset: DataFrame, paramMap: ParamMap): BaggedLogisticRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val oldDataset = extractLabeledPoints(dataset, paramMap)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }
	//var weightsBag : RDD[Vector] = null;
	var weightsBag:Array[Vector] = new Array[Vector](paramMap(bagSize))
	var interceptBag:Array[Double] = new Array[Double](paramMap(bagSize))
	
	println("Dataset size: " + oldDataset.count.toString)
	
	val sc = oldDataset.context
	for( i <- 0 to paramMap(bagSize)-1){
	
		//var resampledSet = sc.union(for( s <- 0 to 9) yield oldDataset.sample(true, 0.1))
		
		val casePatients = oldDataset.filter(x => x.label == 1)
		var resampledCaseSet = sc.union(for( s <- 0 to 9) yield casePatients.sample(true, 0.1))
		
		val notCasePatients = oldDataset.filter(x => x.label == 0)
		var resampledNotCaseSet = sc.union(for( s <- 0 to 9) yield notCasePatients.sample(true, 0.1))
		
		val resampledSet = resampledCaseSet.union(resampledNotCaseSet)		
		
		if (handlePersistence) {
			resampledSet.persist(StorageLevel.MEMORY_AND_DISK)
		}
		//resampledSet.cache()
		
		println("Sample size: " + resampledSet.count.toString)
		
		// Train model
		val lr = new LogisticRegressionWithLBFGS
		lr.optimizer
		  .setRegParam(paramMap(regParam))
          .setNumIterations(paramMap(maxIter))
		var oldModel = lr.run(resampledSet)
		weightsBag(i) = oldModel.weights
		interceptBag(i) = oldModel.intercept
		
		if (handlePersistence) {
			resampledSet.unpersist()
		}
    
	
	}
	
	val lrm = new BaggedLogisticRegressionModel(this, paramMap, weightsBag, interceptBag )
	
    /*// Train model
    val lr = new LogisticRegressionWithLBFGS
    lr.optimizer
      .setRegParam(paramMap(regParam))
      .setNumIterations(paramMap(maxIter))
    val oldModel = lr.run(oldDataset)
    val lrm = new BaggedLogisticRegressionModel(this, paramMap, oldModel.weights, oldModel.intercept)*/

    if (handlePersistence) {
      oldDataset.unpersist()
    }
    lrm
  }
}


/**
 * :: AlphaComponent ::
 *
 * Model produced by [[LogisticRegression]].
 */
@AlphaComponent
class BaggedLogisticRegressionModel private[ml] (
    override val parent: BaggedLogisticRegression,
    override val fittingParamMap: ParamMap,
    val weightsBag: Array[Vector],
    val interceptBag: Array[Double])
  extends ProbabilisticClassificationModel[Vector, BaggedLogisticRegressionModel]
  with BaggedLogisticRegressionParams {

  setThreshold(0.5)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  private val margin: Vector => Array[Double] = (features) => {
	var marginBag:Array[Double] = new Array[Double](paramMap(bagSize))
	for( i <- 0 to paramMap(bagSize)-1){
		marginBag(i)= BLAS.dot(features, weightsBag(i)) + interceptBag(i)
  
	}
	marginBag    
  }

  private val score: Vector => Array[Double] = (features) => {
   var scoreBag:Array[Double] = new Array[Double](paramMap(bagSize))
   val m = margin(features)
	for( i <- 0 to paramMap(bagSize)-1){
		
		scoreBag(i)= 1.0 / (1.0 + math.exp(-m(i)))
  
	}
	scoreBag  
  }
/*
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
      val features2raw = udf { (features: Vector) =>  predictBaggedRaw(features):Array[Vector]}
      tmpData = tmpData.withColumn(map(rawPredictionCol),//features2raw(col(map(featuresCol))))
        callUDF(features2raw, new Array[VectorUDT](map(bagSize)), col(map(featuresCol))))
      numColsOutput += 1
    }
    if (map(probabilityCol) != "") {
      if (map(rawPredictionCol) != "") {
        val raw2prob = udf { (rawPreds: Array[Vector]) =>
		  var probs:Array[Vector] = new Array[Vector](map(bagSize))
		  for( i <- 0 to paramMap(bagSize)-1){
				val prob1 = 1.0 / (1.0 + math.exp(-rawPreds(i)(1)))
				probs(i) = Vectors.dense(1.0 - prob1, prob1)
			}
		  probs
        }
        tmpData = tmpData.withColumn(map(probabilityCol), raw2prob(col(map(rawPredictionCol))))
      } else {
        val features2prob = udf { (features: Vector) => predictBaggedProbabilities(features) : Array[Vector] }
        tmpData = tmpData.withColumn(map(probabilityCol), features2prob(col(map(featuresCol))))
      }
      numColsOutput += 1
    }
    if (map(predictionCol) != "") {
      val t = map(threshold)
      if (map(probabilityCol) != "") {
        val predict = udf { probs: Array[Vector] =>
          var votes = 0
		  for( i <- 1 to map(bagSize)){
				if ( probs(i)(1) > t) 
					votes = votes + 1	
			}
		  
		  if (votes > map(bagSize)/2) 1.0 else 0.0
        }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(probabilityCol))))
      } else if (map(rawPredictionCol) != "") {
        val predict = udf { rawPreds: Array[Vector] =>
		  //var probs:Array[Vector] = new Array[Vector](map(bagSize))
		  var votes = 0
		  for( i <- 1 to map(bagSize)){
				if ( 1.0 / (1.0 + math.exp(-rawPreds(i)(1))) > t) 
					votes = votes + 1	
			}
		  
		  if (votes > map(bagSize)/2) 1.0 else 0.0
        }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(rawPredictionCol))))
      } else {
        val predict = udf { features: Vector => this.predict(features) }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(featuresCol))))
      }
      numColsOutput += 1
    }
    if (numColsOutput == 0) {
      this.logWarning(s"$uid: BaggedLogisticRegressionModel.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    tmpData
  }
*/

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
          val prob1 = 1.0 / (1.0 + math.exp(-rawPreds(1)))
          Vectors.dense(1.0 - prob1, prob1): Vector
        }
        tmpData = tmpData.withColumn(map(probabilityCol), raw2prob(col(map(rawPredictionCol))))
      } else {
        val features2prob = udf { (features: Vector) => predictProbabilities(features) : Vector }
        tmpData = tmpData.withColumn(map(probabilityCol), features2prob(col(map(featuresCol))))
      }
      numColsOutput += 1
    }
    if (map(predictionCol) != "") {
      val t = map(threshold)
      if (map(probabilityCol) != "") {
        val predict = udf { probs: Vector =>
          if (probs(1) > t) 1.0 else 0.0
        }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(probabilityCol))))
      } else if (map(rawPredictionCol) != "") {
        val predict = udf { rawPreds: Vector =>
          val prob1 = 1.0 / (1.0 + math.exp(-rawPreds(1)))
          if (prob1 > t) 1.0 else 0.0
        }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(rawPredictionCol))))
      } else {
        val predict = udf { features: Vector => this.predict(features) }
        tmpData = tmpData.withColumn(map(predictionCol), predict(col(map(featuresCol))))
      }
      numColsOutput += 1
    }
    if (numColsOutput == 0) {
      this.logWarning(s"$uid: LogisticRegressionModel.transform() was called as NOOP" +
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
    val scoreBag = score(features)
	var votes = 0
	for( i <- 0 to paramMap(bagSize)-1){
		if (scoreBag(i) > paramMap(threshold))
			votes = votes + 1 
	}
    if (votes > paramMap(bagSize)/2) 1 else 0
  }

  override protected def predictProbabilities(features: Vector): Vector = {
    val s = score(features)
	//var probs:Array[Vector] = new Array[Vector](paramMap(bagSize))
	var sumProb = 0.0
	for( i <- 0 to paramMap(bagSize)-1){
		sumProb = sumProb + s(i)
	}
    Vectors.dense((paramMap(bagSize) - sumProb)/paramMap(bagSize), sumProb/paramMap(bagSize)) 
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
	//var rawPreds:Array[Vector] = new Array[Vector](paramMap(bagSize))
	var sumMar = 0.0
	for( i <- 0 to paramMap(bagSize)-1){
		sumMar = sumMar + m(i)
	}
	Vectors.dense(0.0, sumMar/paramMap(bagSize))
  }
  
  override protected def copy(): BaggedLogisticRegressionModel = {
    val m = new BaggedLogisticRegressionModel(parent, fittingParamMap, weightsBag, interceptBag)
    Params.inheritValues(this.paramMap, this, m)
    m
  }
}
