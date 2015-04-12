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

/**
 * Big Data Analytics in Healthcare
 * Class Project
 *
 * @author Stephane Collot <stephane.collot@gatech.edu>
 * @author Rishikesh Kulkarni <rissikess@gatech.edu>
 * @author Yannick Le Cacheux <yannick.lecacheux@gatech.edu>
 */

//package edu.gatech.cse8803.CrossValidation

package org.apache.spark.ml.feature

import org.apache.spark.annotation.AlphaComponent
//import org.apache.spark.ml.Transformer
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{VectorUDT, Vector, SparseVector}
import org.apache.spark.sql.types.{DataType, StringType, ArrayType, DoubleType}

/*
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.features.FeatureConstruction
import org.apache.spark.rdd.RDD
*/

import org.apache.spark.Logging
import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * :: AlphaComponent ::
 */
@AlphaComponent
class FeatureTransformer extends UnaryTransformer[Vector, Vector, FeatureTransformer] {

  /**
   * number of features
   * @group param
   */
  val numFeatures = new IntParam(this, "numFeatures", "number of features", Some(1 << 18))

  /** @group getParam */
  def getNumFeatures: Int = get(numFeatures)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

   override protected def createTransformFunc(paramMap: ParamMap): Vector => Vector = { x => x}
   /* val re = paramMap(pattern).r
    val tokens = if (paramMap(gaps)) re.split(str).toSeq else re.findAllIn(str).toSeq
    val minLength = paramMap(minTokenLength)
    tokens.filter(_.length >= minLength)
  }*/

  override protected def outputDataType: DataType = new VectorUDT
}