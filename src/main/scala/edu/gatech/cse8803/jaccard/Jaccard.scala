 /**

  students: please put your implementation in this file!
**/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: String, wd: Double, wm: Double, wl: Double): List[String] = {
    //compute ready state probabilities between patient patientID (NOT VERTEX ID) and all other patients and return the top 10 similar patients
    null
  }

  def summarize(graph: Graph[VertexProperty, EdgeProperty] , patientIDs: List[String]): (List[String], List[String], List[String])  = {

    //replace top_diagnosis with the most frequently used diagnosis in the top most similar patients
    //must contains ICD9 codes
    val top_diagnoses = List("ICD9-1" , "ICD9-2", "ICD9-3", "ICD9-4", "ICD9-5")

    //replace top_medications with the most frequently used medications in the top most similar patients
    //must contain medication name
    val top_medications = List("med_name1" , "med_name2", "med_name3", "med_name4", "med_name5")

    //replace top_labs with the most frequently used labs in the top most similar patients
    //must contain test name
    val top_labs = List("test_name1" , "test_name2", "test_name3", "test_name4", "test_name5")

    (top_diagnoses, top_medications, top_labs)
  }

}