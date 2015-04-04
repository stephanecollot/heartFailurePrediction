/**
 * Big Data Analytics in Healthcare
 * Class Project
 *
 * @author Stephane Collot <stephane.collot@gatech.edu>
 * @author Rishikesh Kulkarni <rissikess@gatech.edu>
 * @author Yannick Le Cacheux <yannick.lecacheux@gatech.edu>
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext._  // Important


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)
  var debug = "12" // 1:count123() 2:count Construct 3:take
  //debug = ""

  /** 1.1
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    constructDiagnosticFeatureTuple(diagnostic, Set())
    //diagnostic.sparkContext.parallelize(List((("patient", "diagnostics"), 1.0)))
  }

  /** 1.2
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    constructMedicationFeatureTuple(medication, Set())
  }

  /** 1.3
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    constructLabFeatureTuple(labResult, Set())
  }
  
  /** 1.4
   * Aggregate feature tuples from vital data, using AVERAGE aggregation
   * @param vital RDD of vital
   * @return RDD of feature tuples
   */
  def constructVitalFeatureTuple(vital: RDD[Vital]): RDD[FeatureTuple] = {
    constructVitalFeatureTuple(vital, Set())
  }

  /** 2.1
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candidateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candidateCode: Set[String]): RDD[FeatureTuple] = {
    var diag = diagnostic
    if(!candidateCode.isEmpty)
        diag = diagnostic.filter(p => candidateCode.contains(p.icd9code))
        
    var map = diag.map(p=> ((p.patientID, p.icd9code), 1.0)).reduceByKey(_ + _)
    
    println("2.1 Diagnostic "+ {if(debug contains "1") map.count+"  dim: "+map.map(p=> p._1._2).distinct.count})
    map
  }

  /** 2.2
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    var med = medication
    if(!candidateMedication.isEmpty)
        med = medication.filter(p => candidateMedication.contains(p.medicine))
        
    var map = med.map(p=> ((p.patientID, p.medicine), 1.0)).reduceByKey(_ + _)
    
    println("2.2 Medication "+ {if(debug contains "1") map.count+"  dim: "+map.map(p=> p._1._2).distinct.count})
    map
  }


  /** 2.3
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
  
    var lab = labResult
    if(!candidateLab.isEmpty)
        lab = labResult.filter(p => candidateLab.contains(p.labName))
        
    var map = lab.map(p=> ((p.patientID, p.labName), p.value))
                 .mapValues(v=> (v, 1.0))
                 .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                 .map(p => (p._1, p._2._1 / p._2._2))
    
    println("2.3 LabResult "+ {if(debug contains "1") map.count+"  dim: "+map.map(p=> p._1._2).distinct.count})
    map
  }
  
  /** 2.4
   * Aggregate feature tuples 
   * @param vital RDD
   * @param candidate set of candidate vital sign -- TODO if needed
   * @return RDD of feature tuples
   */
  def constructVitalFeatureTuple(vital: RDD[Vital], candidate: Set[String]): RDD[FeatureTuple] = {
  
    var vit = vital
    //if(!candidate.isEmpty)
        //vit = vital.filter(p => candidate.contains(p.labName))
        
    var map = vit.map(p=> ((p.patientID, "Weight"), p.Weight))
                 .mapValues(v=> (v, 1.0))
                 .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                 .map(p => (p._1, p._2._1 / p._2._2))
    
    println("2.4 Vital "+ {if(debug contains "1") map.count+"  dim: "+map.map(p=> p._1._2).distinct.count})
    map
  }



  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {
    // FeatureTuple ((String, String), Double)
    /** save for later usage */
    feature.cache()
    
    /** create a feature name to id map*/
    val nameMap = feature.map(p => (p._1._2)).distinct.zipWithIndex.collectAsMap.toMap
    val broadcastfNameMap = sc.broadcast(nameMap)
    val N = broadcastfNameMap.value.size
    println("number of fName:"+N) //210
    if(debug contains "3")
      for (p <- broadcastfNameMap.value.take(5))
        println(p.toString)

    
    /** transform input feature */
    // Functions maybe helpful:  collect  groupByKey
    val featureMap = feature.map(p => (p._1._1, (lookUpSeqToInt(broadcastfNameMap.value, p._1._2), p._2)))
    val featureGrouped = featureMap.groupByKey
    println("featureGrouped:"+{if(debug contains "2") featureGrouped.count}) //
    if(debug contains "3")
      for (p <- featureGrouped.take(5))
        println(p.toString)

    val groupedVectoried = featureGrouped.map(x => (x._1, Vectors.sparse(N, x._2.toSeq)))
    println("groupedVectoried:"+{if(debug contains "2") groupedVectoried.count}) //
    if(debug contains "3")
      for (p <- groupedVectoried.take(5))
        println(p.toString)
    

    //sc.parallelize(Seq(("Patient-NO-1", Vectors.dense(1.0, 2.0))))
    groupedVectoried
  }

  /**
   * lookup/get().head.toInt
   * @param Map[String, Long]
   * @return Int value of the key
   */
  def lookUpSeqToInt(fNameMap: Map[String, Long], p2: String): Int = {
    if(p2.isEmpty) {
      println("Error Key empty")
      0
    }
    else {
      var seq = fNameMap.get(p2)
      if(seq.isEmpty) {
        println("Error Seq empty")
        0
      }
      else {
          seq.head.toInt
      }
    }
  }

}


