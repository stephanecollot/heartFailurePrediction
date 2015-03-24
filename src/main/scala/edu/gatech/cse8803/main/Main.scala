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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._  // Important
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import java.text.SimpleDateFormat

//Change log level
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  def parseDouble(s: String) = {
    try {
      s.toDouble
    } 
    catch { 
      case _ => 0.0
    }
  }

  def main(args: Array[String]) {
  
    //Change log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val sc = createContext
    val sqlContext = new SQLContext(sc)
    
    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext)
    
  
    var f1 = diagnostic.filter(d => (parseDouble(d.icd9code) > 390.0 && parseDouble(d.icd9code) < 495.0))
    println("heart count: "+f1.count)
    
    var f2 = f1.map(d => (d.patientID, d.icd9code))
    var g1 = f2.groupByKey
    println("patient heart count: "+g1.count)
    
    var g2 = diagnostic.map(d => (d.patientID, d.icd9code)).groupByKey
    println("patient TOTAL count: "+g2.count)
    
    var m1 = f1.map(d => (d.icd9code, 1)).reduceByKey{case (x1, x2) => (x1 + x2)}
    m1.foreach(println)

  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") //dateFormat.parse()
    
    val SchemaRDDmed = CSVUtils.loadCSVAsTable(sqlContext, "data/medication_fulfillment.csv")
    //case class Medication(patientID: String, date: Long, medicine: String)
    var med = SchemaRDDmed.map(p => Medication(p(2).toString, dateFormat.parse(p(6).toString).getTime(), p(7).toString))
    println("med"+med.count())
    
    val SchemaRDDlab = CSVUtils.loadCSVAsTable(sqlContext, "data/lab_results.csv")
    //case class LabResult(patientID: String, date: Long, labName: String, loincCode: String, value: String)
    var lab = SchemaRDDlab.map(p => LabResult(p(1).toString, dateFormat.parse(p(2).toString).getTime(), p(7).toString, p(10).toString, p(14).toString))
    println("lab"+lab.count())
    
    val SchemaRDDenc = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter.csv")
    // (encounterID: String, (patientID: String, date: Long))
    var enc = SchemaRDDenc.map(p => (p(1).toString, (p(2).toString, dateFormat.parse(p(6).toString).getTime())))
    println("enc"+enc.count())
    
    val SchemaRDDencDX = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_dx.csv")
    //(encounterID: String, icd9code: String)
    var encDx = SchemaRDDencDX.map(p => (p(5).toString, p(1).toString))
    println("encDx"+encDx.count())
    
    //case class Diagnostic(patientID: String, date: Long, icd9code: String)
    var diag = enc.join(encDx).map(p => Diagnostic(p._2._1._1, p._2._1._2, p._2._2))
  
    println("lab: "+lab.count()+"  diag: "+diag.count()+"  med: "+med.count())
    //lab:   diag:   med:  
  
    (med, lab, diag)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Project", "local")
}
