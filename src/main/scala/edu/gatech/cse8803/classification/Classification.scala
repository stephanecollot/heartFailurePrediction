/**
 * Big Data Analytics in Healthcare
 * Class Project
 *
 * @author Stephane Collot <stephane.collot@gatech.edu>
 * @author Rishikesh Kulkarni <rissikess@gatech.edu>
 * @author Yannick Le Cacheux <yannick.lecacheux@gatech.edu>
 */

package edu.gatech.cse8803.classification

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

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.regression.LabeledPoint


object Classification {
	
	def classify(input: RDD[DataSet]) {
		println("Doing classification")
		
		val labeled = input.map(x => new LabeledPoint(x.label, x.featureVector))
		println("Number of patients: " + labeled.count())
		
		val casePatients = labeled.filter(x => x.label == 1)
		val nbrCasePatients = casePatients.count()
		println("Number of case patients: " + nbrCasePatients)
		val notCasePatients = labeled.filter(x => x.label ==0)
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
		
		/*
		val testing = labeled.sample(false, 0.2, 45897).cache()
		val training = labeled.subtract(testing).cache()
		
		//Log info for debug
		println("testing set size: " + testing.count())
		println("training set size: " + training.count())
		val numberOfCasePatientsInTrainingSet = training.filter(x => x.label == 1).count
		println("number of case patients in training set: " + numberOfCasePatientsInTrainingSet)
		val numberOfCasePatientsInTestingSet = testing.filter(x => x.label == 1).count
		println("number of case patients in testing set: " + numberOfCasePatientsInTestingSet)
		* */
		
		val model = LogisticRegressionWithSGD.train(trainingSet, 20)
		
		val labelAndPreds = testingSet.map { point =>
			val prediction = model.predict(point.features)
			(point.label, prediction)
		}
		
		val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testingSet.count
		println("Training Error = " + trainErr)
	}
	
}
