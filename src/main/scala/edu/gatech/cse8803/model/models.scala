/**
 * Big Data Analytics in Healthcare
 * Class Project
 *
 * @author Stephane Collot <stephane.collot@gatech.edu>
 * @author Rishikesh Kulkarni <rissikess@gatech.edu>
 * @author Yannick Le Cacheux <yannick.lecacheux@gatech.edu>
 */
 
package edu.gatech.cse8803.model

case class LabResult(patientID: String, date: Long, labName: String, loincCode: String, value: Double)

case class Diagnostic(patientID: String, date: Long, icd9code: String) //, sequence: Int

case class Medication(patientID: String, date: Long, medicine: String)

