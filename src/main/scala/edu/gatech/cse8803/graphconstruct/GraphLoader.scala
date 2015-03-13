/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    null
  }

  def runPageRank(graph:  Graph[VertexProperty, EdgeProperty] ): List[(String, Double)] ={
    //run pagerank provided by GraphX
    //return the top 5 mostly highly ranked vertices
    //for each vertex return the vertex name, which can be patientID, test_name or medication name and the corresponding rank
    //see example below
    val p = List(("vertex_name1", 0.3), ("vertex_name2", 0.1), ("vertex_name2", 0.2), ("vertex_name4", 0.1), ("vertex_name5", 0.1))
    p
  }
}
