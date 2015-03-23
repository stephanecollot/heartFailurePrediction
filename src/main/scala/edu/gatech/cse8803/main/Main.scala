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
  def main(args: Array[String]) {
  
    //Change log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val sc = createContext
    val sqlContext = new SQLContext(sc)
    
    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext)


  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX") //dateFormat.parse()
    
    val SchemaRDDmed = CSVUtils.loadCSVAsTable(sqlContext, "data/medication_fulfillment.csv")
    //case class Medication(patientID: String, date: Long, medicine: String)
    var med = SchemaRDDmed.map(p => Medication(p(2).toString, dateFormat.parse(p(6).toString).getTime(), p(7).toString))
    
    val SchemaRDDlab = CSVUtils.loadCSVAsTable(sqlContext, "data/lab_results.csv")
    //case class LabResult(patientID: String, date: Long, labName: String, loincCode: String, value: String)
    var lab = SchemaRDDlab.map(p => LabResult(p(1).toString, dateFormat.parse(p(2).toString).getTime(), p(7).toString, p(10).toString, p(14).toString))
    
    val SchemaRDDenc = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter.csv")
    // (encounterID: String, (patientID: String, date: Long))
    var enc = SchemaRDDenc.map(p => (p(1).toString, (p(2).toString, dateFormat.parse(p(6).toString).getTime())))
    
    val SchemaRDDencDX = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_dx.csv")
    //(encounterID: String, icd9code: String)
    var encDx = SchemaRDDencDX.map(p => (p(5).toString, p(1).toString))
    
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
