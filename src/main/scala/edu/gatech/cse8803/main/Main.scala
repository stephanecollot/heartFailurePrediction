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
import edu.gatech.cse8803.classification.Classification
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._  // Important
import org.apache.spark.{SparkConf, SparkContext}

import edu.gatech.cse8803.CrossValidation.CrossValidation

import scala.io.Source
import java.text.SimpleDateFormat

//Change log level
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {
  var arguments = Array[String]()

  def parseDouble(s: String) = {
    try {
      s.toDouble
    } 
    catch { 
      case e: Throwable => 0.0
    }
  }
  
  def convertHeight(v: Double, u: String): Double = {
    if (u=="cm")
      v/100
    else if (u=="in")
      v/39.370
    else {
      println("UNKOWN UNIT:"+u)
      0.0
    }
  }
  
  def convertWeight(v: Double, u: String): Double = {
    if (u=="kg")
      v
    else if (u=="lbs")
      v/2.20462262185
    else {
      println("UNKOWN UNIT:"+u)
      0.0
    }
  }
  
  def BMI(height: Double, heightUnit: String, weight: Double, weightUnit: String): Double = {
    val h = convertHeight(height, heightUnit)
    val w = convertWeight(weight, weightUnit)
    
    var value = 0.0
    if (h!=0.0)
      value = w/(h*h)
      
    //println("BMI value: "+value)
    value
  }

  def main(args: Array[String]) {
  
    val sysTime = System.nanoTime
  
    //Change log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    println("Arguments: ")
    args.foreach(println)
    arguments = args
    
    val sc = createContext
    val sqlContext = new SQLContext(sc)
    
    /** Initialize loading of data */
    val (medication, labResult, diagnostic, vital) = loadRddRawData(sqlContext)
    
    /** Information Display */
    var f1 = diagnostic.filter(d => (parseDouble(d.icd9code) > 390.0 && parseDouble(d.icd9code) < 495.0))
    println("heart count: "+f1.count)
    
    var g1 = f1.map(d => (d.patientID, 1))
							 .reduceByKey{case (x1, x2) => (x1 + x2)}
    println("patient heart count: "+g1.count)
    
    var g2 = diagnostic.map(d => (d.patientID, 1))
											 .reduceByKey{case (x1, x2) => (x1 + x2)}
    println("patient TOTAL count: "+g2.count)
    
    var m1 = f1.map(d => (d.icd9code, 1))
							 .reduceByKey{case (x1, x2) => (x1 + x2)}
    m1.foreach(println)
        
    /** Feature construction with all features */
    var targetCode = "428.0" //(428.0,91) Congestive heart failure, unspecified
    // Remove all event after this first diag
    var patientDate = diagnostic.filter(d => (d.icd9code == targetCode)) // this constains only label=1 patient
                                .map(d => (d.patientID, d.date)) // (patientID, date)
                                .reduceByKey( Math.min(_,_))     // (patientID, maxDate)
    /*println("patient with targetCode: " + targetCode.toString + " count: "+patientDate.count)
    
    var diagMinMax = diagnostic.map(d => ((d.date, d.date))) // (patientID, date, date)
                               .reduce( (x,y) => (Math.min(x._1, y._1), Math.max(x._2,y._2))) // (patientID, (minDate, maxDate))
    println("diag Min & Max date: " + diagMinMax.toString)*/
  
    val predictWindow = 86400000 * 365      
    var diagnosticF = diagnostic.map(d => (d.patientID, d))  // (patientID, diag)
                                .leftOuterJoin(patientDate)  // (patientID, (diag, maxDate)) or (diag, None)
                                .filter(d => d._2._2 match {case Some(value) =>
                                                              (d._2._1.date < value-predictWindow)
                                                            case None =>
                                                              true
                                                           })
                                .map(d => d._2._1)
     
    var labResultF = labResult.map(d => (d.patientID, d))  // (patientID, lab)
                              .leftOuterJoin(patientDate)  // (patientID, (lab, maxDate)) or (lab, None)
                              .filter(d => d._2._2 match {case Some(value) =>
                                                            (d._2._1.date < value-predictWindow)
                                                          case None =>
                                                            true
                                                         })
                              .map(d => d._2._1)
     
    var medicationF = medication.map(d => (d.patientID, d))  // (patientID, med)
                                .leftOuterJoin(patientDate)  // (patientID, (med, maxDate)) or (med, None)
                                .filter(d => d._2._2 match {case Some(value) =>
                                                              (d._2._1.date < value-predictWindow)
                                                            case None =>
                                                              true
                                                           })
                                .map(d => d._2._1)
     
    var vitalF = vital.map(d => (d.patientID, d))  // (patientID, vital)
                      .leftOuterJoin(patientDate)  // (patientID, (vital, maxDate)) or (vital, None)
                      .filter(d => d._2._2 match {case Some(value) =>
                                                    (d._2._1.date < value-predictWindow)
                                                  case None =>
                                                    true
                                                 })
                      .map(d => d._2._1)
                                
    println("FILTERED lab: "+labResultF.count()+"  diag: "+diagnosticF.count()+"  med: "+medicationF.count()+"  vital: "+vitalF.count())
    
    var featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnosticF), // -> 0.9941176470588236
      FeatureConstruction.constructLabFeatureTuple(labResultF), //-> 0.956985294117647
      FeatureConstruction.constructMedicationFeatureTuple(medicationF), // -> 0.988235294117647
      FeatureConstruction.constructVitalFeatureTuple(vitalF) // -> 0.5
    )
    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    // Create (patientID, label, vector)
    var patient1 = patientDate.map(p => (p._1, 1)) // (patientID, 1)
    var patientAll = diagnostic.map(p => (p.patientID, 0)) // (patientID, 0)
    var patientLabel = patientAll.union(patient1).reduceByKey( Math.max(_,_)) // (patientID, label)
    println("number of patient: " + patientLabel.count)
    
    var allfeats = patientLabel.join(rawFeatures)
    var inputClassifier = allfeats .map(p => (p._1, p._2._1, p._2._2)) // (patientID, label, vector)
    var inputCV = allfeats.map(p => DataSet(p._1, p._2._1, p._2._2)) // (patientID, label, vector)
    println("inputClassifier.take(5):")
    inputClassifier.take(5).toList.foreach(println)
    
    //Classification
    //Classification.classify(inputClassifier) // Basic without pipeline

    CrossValidation.crossValidate(inputCV, sc, sqlContext, arguments)
    println("Done2")
    var time = (System.nanoTime-sysTime)/(1e6*1000)
    println("time: "+time.toString+"s")
    sc.stop()
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic], RDD[Vital]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") //dateFormat.parse()
    
    var path = "data/"
    if (arguments.contains("big"))
      path = "data/GeorgiaTech_DS1_CSV/"
    
    val SchemaRDDmed = CSVUtils.loadCSVAsTable(sqlContext, path+"medication_fulfillment.csv")
    //case class Medication(patientID: String, date: Long, medicine: String)
    var med = SchemaRDDmed.map(p => Medication(p(2).toString, dateFormat.parse(p(6).toString).getTime(), p(7).toString))
    println("med: "+med.count())
    
    val SchemaRDDlab = CSVUtils.loadCSVAsTable(sqlContext, path+"lab_results.csv")
    //case class LabResult(patientID: String, date: Long, labName: String, loincCode: String, value: Double)
    var lab = SchemaRDDlab.map(p => LabResult(p(1).toString, dateFormat.parse(p(2).toString).getTime(), p(7).toString, p(10).toString, parseDouble(p(14).toString)))
    println("lab: "+lab.count())
    
    val SchemaRDDenc = CSVUtils.loadCSVAsTable(sqlContext, path+"encounter.csv")
    // (encounterID: String, (patientID: String, date: Long))
    var enc = SchemaRDDenc.map(p => (p(1).toString, (p(2).toString, dateFormat.parse(p(6).toString).getTime())))
    println("enc: "+enc.count())
		
    if (arguments.contains("out")) {
      val SchemaRDDencOut = CSVUtils.loadCSVAsTable(sqlContext, path+"encounter_outpatient.csv")
      // (encounterID: String, (patientID: String, date: Long))
      var encOut = SchemaRDDencOut.map(p => (p(1).toString, (p(2).toString, dateFormat.parse(p(5).toString).getTime())))
      println("encOut: "+encOut.count())
      enc = enc.union(encOut)
      println("encMerged: "+enc.count())
    }

    
    val SchemaRDDencDX = CSVUtils.loadCSVAsTable(sqlContext, path+"encounter_dx.csv")
    //(encounterID: String, icd9code: String)
    var encDx = SchemaRDDencDX.map(p => (p(5).toString, p(1).toString))
    println("encDx: "+encDx.count())
    
    //case class Diagnostic(patientID: String, date: Long, icd9code: String)
    var diag = enc.join(encDx).map(p => Diagnostic(p._2._1._1, p._2._1._2, p._2._2))
    
    val SchemaRDDvital = CSVUtils.loadCSVAsTable(sqlContext, path+"vital_sign.csv")
    //case class Vital(patientID: String, date: Long, Height: Double, Weight: Double, SystolicBP: Double, DiastolicBP: Double, Pulse: Double, Respiration: Double, Temperature: Double, BMI: Double)
    var vital = SchemaRDDvital.map(p => Vital(p(1).toString, dateFormat.parse(p(2).toString).getTime(), convertHeight(parseDouble(p(3).toString), p(4).toString), convertWeight(parseDouble(p(5).toString), p(6).toString), parseDouble(p(7).toString), parseDouble(p(8).toString), parseDouble(p(9).toString), parseDouble(p(10).toString), parseDouble(p(11).toString), BMI(parseDouble(p(3).toString),p(4).toString,parseDouble(p(5).toString),p(6).toString)))
     
    println("lab: "+lab.count()+"  diag: "+diag.count()+"  med: "+med.count()+"  vital: "+vital.count())
  
    (med, lab, diag, vital)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Project", "local")
}
