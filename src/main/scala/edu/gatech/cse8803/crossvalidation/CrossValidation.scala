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



import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics // For ROC
import org.apache.spark.mllib.evaluation.MulticlassMetrics // For confusion matrix
import org.apache.spark.sql.{Row, SQLContext}

import org.apache.spark.ml.feature.FeatureTransformer
import org.apache.spark.ml.classification.BaggedLogisticRegression

import org.apache.spark.ml.classification.RandomForestForPipeline

object CrossValidation {

  def crossValidate(data: RDD[DataSet], sc: SparkContext, sqlContext: SQLContext, arguments: Array[String]) = {
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
		
		val testingCase = casePatients.sample(false, fractionInTestingSet).cache()
		val testingCaseSize = testingCase.count()
		val trainingCase = casePatients.subtract(testingCase).cache()
		val trainingCaseSize = trainingCase.count()
		println("Number of case patients in training set: " + trainingCaseSize)
		println("Number of case patients in testing set: " + testingCaseSize)
		
		val trainingSet = trainingCase.union(notCasePatients.sample(false, trainingCaseSize.toDouble / nbrNotCasePatients))
		val testingSet = testingCase.union(notCasePatients.sample(false, (1.0-(trainingCaseSize.toDouble / nbrNotCasePatients) )))

		/*val splits = data.randomSplit(Array(0.6, 0.4), seed = System.currentTimeMillis().toInt)
		val trainingSet = splits(0)
		val testingSet = splits(1)*/


		println("Training set size: " + trainingSet.count)
		println("Testing set size: " + testingSet.count)
		
	  
	  
    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    /*val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")*/
	  
    val FeatureTransformer = new FeatureTransformer()
      .setInputCol("featureVector")
      .setOutputCol("features")
	
	/*-------------------------------------------------------------Logistic Regression---------------------------------------------------*/
	
			
		var lr = new LogisticRegression()
			.setMaxIter(10)
		var lrBagged = new BaggedLogisticRegression()
			.setMaxIter(10)
		
    val pipeline = new Pipeline()
		if (arguments.contains("nobag")) {
      pipeline.setStages(Array(FeatureTransformer, lr))
		}
		else {
      pipeline.setStages(Array(FeatureTransformer, lrBagged))
		}
			

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val crossval = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
	  
	  
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    var paramGrid = new ParamGridBuilder()
      .addGrid(FeatureTransformer.numFeatures, Array(10, 100, 190))
    if (arguments.contains("nobag")) {
      paramGrid.addGrid(lr.regParam, Array(0.1, 0.01))
    }
    else {
      paramGrid.addGrid(lrBagged.regParam, Array(0.1, 0.01))
      paramGrid.addGrid(lrBagged.bagSize, Array(1))
		}
    crossval.setEstimatorParamMaps(paramGrid.build())
    crossval.setNumFolds(3) // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = crossval.fit(trainingSet.toDF())

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val testingResults = cvModel.transform(testingSet.toDF())
                                .select("patientID", "prediction")
                                .map({case Row(patientID: String, prediction: Double) => (patientID.toString,prediction.toDouble)})
	  
    val trainingResults = cvModel.transform(trainingSet.toDF())
                                 .select("patientID", "prediction")
                                 .map({case Row(patientID: String, prediction: Double) => (patientID.toString,prediction.toDouble)})
                                
    val testingEstimatesLabels = testingResults.join(testingSet.map(x => (x.patientID, x.label))) // (patientID, (estimate, label))
                                               .map { r => (r._2._1.toDouble, r._2._2.toDouble) } // (estimate, label)
                                               
    val trainingEstimatesLabels = trainingResults.join(trainingSet.map(x => (x.patientID, x.label))) // (patientID, (estimate, label))
                                                 .map { r => (r._2._1.toDouble, r._2._2.toDouble) } // (estimate, label)
        
    // Get evaluation metrics.
    val testingBinaryMetrics = new BinaryClassificationMetrics(testingEstimatesLabels)
    val testingMulticlassMetrics = new MulticlassMetrics(testingEstimatesLabels)
    
    val trainingBinaryMetrics = new BinaryClassificationMetrics(trainingEstimatesLabels)
    val trainingMulticlassMetrics = new MulticlassMetrics(trainingEstimatesLabels)
    
    // Get metrics values
    val testingAccuracy = testingMulticlassMetrics.precision
    val testingConfusion = testingMulticlassMetrics.confusionMatrix
    val testingAUROC = testingBinaryMetrics.areaUnderROC()
    val testingROC = testingBinaryMetrics.roc()
    
    val trainingAccuracy = trainingMulticlassMetrics.precision
    val trainingConfusion = trainingMulticlassMetrics.confusionMatrix
    val trainingAUROC = trainingBinaryMetrics.areaUnderROC()
    val trainingROC = trainingBinaryMetrics.roc()

    // Print results
    println("Testing:")
    println("Testing Accuracy: " + testingAccuracy.toString)
    println("Testing Confusion: ")
    println(testingConfusion.toString)
    println("Testing AUROC: " + testingAUROC.toString)
    print("Testing ROC: ")
    testingROC.foreach(x => print("[" + x._1.toString + ", " + x._2.toString + "] " ) )
    println("")
    
    println("Training:")
    println("Training Accuracy: " + trainingAccuracy.toString)
    println("Training Confusion: ")
    println(trainingConfusion.toString)
    println("Training AUROC: " + trainingAUROC.toString)
    print("Training ROC: ")
    trainingROC.foreach(x => print("[" + x._1.toString + ", " + x._2.toString + "] " ) )
    println("")
    
    //ROCRDD.saveAsTextFile("ROC")
	
	
	/*------------------------------------------Random Forest-------------------------------------------------*/
	
	
	
	var rf = new RandomForestForPipeline()
	
		
    val rfpipeline = new Pipeline()
	rfpipeline.setStages(Array(FeatureTransformer, rf))
	
	val rfcrossval = new CrossValidator()
      .setEstimator(rfpipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
	
    var rfparamGrid = new ParamGridBuilder()
      .addGrid(FeatureTransformer.numFeatures, Array(10, 100, 190))
	  .addGrid(rf.strategy, Array("Classification"))
	  .addGrid(rf.numTrees, Array(3, 9))
	  .addGrid(rf.featureSubsetStrategy, Array("auto"))
	  
		
    rfcrossval.setEstimatorParamMaps(rfparamGrid.build())
    rfcrossval.setNumFolds(3) // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val rfcvModel = rfcrossval.fit(trainingSet.toDF())

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val rftestingResults = rfcvModel.transform(testingSet.toDF())
                                .select("patientID", "prediction")
                                .map({case Row(patientID: String, prediction: Double) => (patientID.toString,prediction.toDouble)})
	  
    val rftrainingResults = rfcvModel.transform(trainingSet.toDF())
                                 .select("patientID", "prediction")
                                 .map({case Row(patientID: String, prediction: Double) => (patientID.toString,prediction.toDouble)})
                                
    val rftestingEstimatesLabels = rftestingResults.join(testingSet.map(x => (x.patientID, x.label))) // (patientID, (estimate, label))
                                               .map { r => (r._2._1.toDouble, r._2._2.toDouble) } // (estimate, label)
                                               
    val rftrainingEstimatesLabels = rftrainingResults.join(trainingSet.map(x => (x.patientID, x.label))) // (patientID, (estimate, label))
                                                 .map { r => (r._2._1.toDouble, r._2._2.toDouble) } // (estimate, label)
        
    // Get evaluation metrics.
    val rftestingBinaryMetrics = new BinaryClassificationMetrics(rftestingEstimatesLabels)
    val rftestingMulticlassMetrics = new MulticlassMetrics(rftestingEstimatesLabels)
    
    val rftrainingBinaryMetrics = new BinaryClassificationMetrics(rftrainingEstimatesLabels)
    val rftrainingMulticlassMetrics = new MulticlassMetrics(rftrainingEstimatesLabels)
    
    // Get metrics values
    val rftestingAccuracy = rftestingMulticlassMetrics.precision
    val rftestingConfusion = rftestingMulticlassMetrics.confusionMatrix
    val rftestingAUROC = rftestingBinaryMetrics.areaUnderROC()
    val rftestingROC = rftestingBinaryMetrics.roc()
    
    val rftrainingAccuracy = rftrainingMulticlassMetrics.precision
    val rftrainingConfusion = rftrainingMulticlassMetrics.confusionMatrix
    val rftrainingAUROC = rftrainingBinaryMetrics.areaUnderROC()
    val rftrainingROC = rftrainingBinaryMetrics.roc()

    // Print results
	println("=========================================Random Forest====================================")
    println("Testing:")
    println("Testing Accuracy: " + rftestingAccuracy.toString)
    println("Testing Confusion: ")
    println(rftestingConfusion.toString)
    println("Testing AUROC: " + rftestingAUROC.toString)
    print("Testing ROC: ")
    rftestingROC.foreach(x => print("[" + x._1.toString + ", " + x._2.toString + "] " ) )
    println("")
    
    println("Training:")
    println("Training Accuracy: " + rftrainingAccuracy.toString)
    println("Training Confusion: ")
    println(rftrainingConfusion.toString)
    println("Training AUROC: " + rftrainingAUROC.toString)
    print("Training ROC: ")
    rftrainingROC.foreach(x => print("[" + x._1.toString + ", " + x._2.toString + "] " ) )
    println("")
    
  }
}
