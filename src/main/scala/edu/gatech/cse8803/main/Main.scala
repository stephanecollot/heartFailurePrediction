/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.graphconstruct.GraphLoader
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.jaccard._
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.randomwalk._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object Main {
  def main(args: Array[String]) {
    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (patient, medication, labResult, diagnostic) = loadRddRawData(sqlContext)

    //build the graph
    val graph = GraphLoader.load( patient, labResult, medication, diagnostic )

    //compute pagerank
    testPageRank(graph)

    //Jaccard using only diagnosis
    testJaccard(graph, 1, 0, 0)

    //Weighted Jaccard
    testJaccard(graph, 0.5, 0.3, 0.2)

    //Random walk similarity
    testRandomWalk(graph)
  }

  def testJaccard( graphInput:  Graph[VertexProperty, EdgeProperty], wd: Double, wm: Double, wl: Double ) = {
    val patientIDtoLookup = "5"

    val answerTop10patients = Jaccard.jaccardSimilarityOneVsAll(graphInput, patientIDtoLookup, wd, wm, wl)
    val (answerTop10med, answerTop10diag, answerTop10lab) = Jaccard.summarize(graphInput, answerTop10patients)
    /* compute Jaccard coefficient on the graph */
    println("the top 10 most similar patients are: ")
    // print the patinet IDs here
    answerTop10patients.foreach(println)
    println("the top 10 meds, diagnoes, and labs for these 10 patients are: ")
    //print the meds, diagnoses and labs here
    answerTop10med.foreach(println)
    answerTop10diag.foreach(println)
    answerTop10lab.foreach(println)
    null
  }

  def testRandomWalk( graphInput:  Graph[VertexProperty, EdgeProperty] ) = {
    val patientIDtoLookup = "5"
    val answerTop10patients = RandomWalk.randomWalkOneVsAll(graphInput, patientIDtoLookup)
    val (answerTop10med, answerTop10diag, answerTop10lab) = RandomWalk.summarize(graphInput, answerTop10patients)
    /* compute Jaccard coefficient on the graph */
    println("the top 10 most similar patients are: ")
    // print the patinet IDs here
    answerTop10patients.foreach(println)
    println("the top 10 meds, diagnoes, and labs for these 10 patients are: ")
    //print the meds, diagnoses and labs here
    answerTop10med.foreach(println)
    answerTop10diag.foreach(println)
    answerTop10lab.foreach(println)
    null
  }

  def testPageRank( graphInput:  Graph[VertexProperty, EdgeProperty] ) = {
    //run pagerank provided by GraphX
    //print the top 5 mostly highly ranked vertices
    //for each vertex print the vertex name, which can be patientID, test_name or medication name and the corresponding rank
    val p = GraphLoader.runPageRank(graphInput)
    p.foreach(println)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientProperty], RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {

    (null, null, null, null)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
