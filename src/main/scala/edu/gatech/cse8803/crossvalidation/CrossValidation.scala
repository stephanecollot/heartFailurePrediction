/**
 * Big Data Analytics in Healthcare
 * Class Project
 *
 * @author Stephane Collot <stephane.collot@gatech.edu>
 * @author Rishikesh Kulkarni <rissikess@gatech.edu>
 * @author Yannick Le Cacheux <yannick.lecacheux@gatech.edu>
 */

package edu.gatech.cse8803.CrossValidation

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
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics // For ROC
import org.apache.spark.sql.{Row, SQLContext}

import org.apache.spark.ml.feature.FeatureTransformer


object CrossValidation {

  def crossValidate(data: RDD[DataSet], sc:SparkContext, sqlContext : SQLContext): Double = {
    /*val conf = new SparkConf().setAppName("CrossValidatorExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)*/
    import sqlContext.implicits._
	  
	  
	  //val labeled = data.map(x => new LabeledPoint(x._2, x._3))
	    println("Number of patients: " + data.count())
		
		val casePatients = data.filter(x => x.label == 1)
		val nbrCasePatients = casePatients.count()
		println("Number of case patients: " + nbrCasePatients)
		val notCasePatients = data.filter(x => x.label == 0)
		val nbrNotCasePatients = notCasePatients.count()
		println("Number of not case patients: " + nbrNotCasePatients)
		
		val fractionInTestingSet = 0.2
		
		val testingCase = casePatients.sample(false, fractionInTestingSet, 12345).cache()
		val testingCaseSize = testingCase.count()
		val trainingCase = casePatients.subtract(testingCase).cache()
		val trainingCaseSize = trainingCase.count()
		println("Number of case patients in training set: " + trainingCaseSize)
		println("Number of case patients in testing set: " + testingCaseSize)
		
		val trainingSet = trainingCase.union(notCasePatients.sample(false, trainingCaseSize.toDouble / nbrNotCasePatients, 12345))
		val testingSet = testingCase.union(notCasePatients.sample(false, (testingCaseSize.toDouble + 1.0) / nbrNotCasePatients, 54321))
		
		println("Training set size: " + trainingSet.count)
		println("Testing set size: " + testingSet.count)
		
	  
	  
    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    /*val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")*/
	  
    val FeatureTransformer = new FeatureTransformer()
      .setInputCol("featureVector")
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val pipeline = new Pipeline()
      .setStages(Array(FeatureTransformer, lr))

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
      .addGrid(FeatureTransformer.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()
    crossval.setEstimatorParamMaps(paramGrid)
    crossval.setNumFolds(2) // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = crossval.fit(trainingSet.toDF())

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val results = cvModel.transform(testingSet.toDF())
      .select("patientID", "prediction")
	  .map({case Row(patientID: String, prediction: Double) => (patientID.toString,prediction.toDouble)})
	  
      /*.foreach { case Row(patientID: Long, prob: Vector, prediction: Double) =>
      println(s"($patientID) --> prob=$prob, prediction=$prediction")
    }*/
	
	val labeled = testingSet.map(x => (x.patientID, x.label))
	
	val labelAndPreds = results.join(labeled) // (PatientID, (prediction, label))
	
	val trainErr = labelAndPreds.filter(r => r._2._1 != r._2._2).count.toDouble / testingSet.count
		println("Training Error = " + trainErr)
	
  // Compute raw scores on the test set. 
  val scoreAndLabels = labelAndPreds.map { r => (r._2._1.toDouble, r._2._2.toDouble) }
  // Get evaluation metrics.
  val metrics = new BinaryClassificationMetrics(scoreAndLabels)
  val auROC = metrics.areaUnderROC()
  println("Area Under Receiver Operating Characteristic (ROC curve): " + auROC.toString)
  val ROCRDD = metrics.roc()
  println("ROC curve: ")
  ROCRDD.foreach (x => println(x._1.toString + ", " + x._2.toString ) )
  //ROCRDD.saveAsTextFile("ROC")
  
	trainErr
    //sc.stop()
  }
}
