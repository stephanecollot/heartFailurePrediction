/**
 * Big Data Analytics in Healthcare
 * Class Project
 *
 * @author Stephane Collot <stephane.collot@gatech.edu>
 * @author Rishikesh Kulkarni <rissikess@gatech.edu>
 * @author Yannick Le Cacheux <yannick.lecacheux@gatech.edu>
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.features.FeatureConstruction
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._  // Important
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import java.text.SimpleDateFormat

//Change log level
import org.apache.log4j.Logger
import org.apache.log4j.Level


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}


object CrossValidation {

  def crossValidate(data: RDD[DataSet]) {
    val conf = new SparkConf().setAppName("CrossValidatorExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare training documents, which are labeled.
    val training = sc.parallelize(Seq(
      LabeledDocument(0L, "a b c d e spark", 1.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(2L, "spark f g h", 1.0),
      LabeledDocument(3L, "hadoop mapreduce", 0.0),
      LabeledDocument(4L, "b spark who", 1.0),
      LabeledDocument(5L, "g d a y", 0.0),
      LabeledDocument(6L, "spark fly", 1.0),
      LabeledDocument(7L, "was mapreduce", 0.0),
      LabeledDocument(8L, "e spark program", 1.0),
      LabeledDocument(9L, "a e c l", 0.0),
      LabeledDocument(10L, "spark compile", 1.0),
      LabeledDocument(11L, "hadoop software", 0.0)))

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val crossval = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()
    crossval.setEstimatorParamMaps(paramGrid)
    crossval.setNumFolds(2) // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = crossval.fit(training.toDF())

    // Prepare test documents, which are unlabeled.
    val test = sc.parallelize(Seq(
      Document(4L, "spark i j k"),
      Document(5L, "l m n"),
      Document(6L, "mapreduce spark"),
      Document(7L, "apache hadoop")))

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(test.toDF())
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }

    sc.stop()
  }
}
  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Project", "local")
}
