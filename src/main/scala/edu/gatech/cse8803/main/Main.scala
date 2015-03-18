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
    //val graph = GraphLoader.load( patient, labResult, medication, diagnostic )
    /*
    //compute pagerank
    testPageRank(graph)
    
    //Jaccard using only diagnosis
    testJaccard(graph, 1, 0, 0)

    //Weighted Jaccard
    testJaccard(graph, 0.5, 0.3, 0.2)

    //Random walk similarity
    testRandomWalk(graph)
    */
  }
  /*
  def testJaccard( graphInput:  Graph[VertexProperty, EdgeProperty], wd: Double, wm: Double, wl: Double ) = {
    val patientIDtoLookup = "5"

    val answerTop10patients = Jaccard.jaccardSimilarityOneVsAll(graphInput, patientIDtoLookup, wd, wm, wl)
    val (answerTop10med, answerTop10diag, answerTop10lab) = Jaccard.summarize(graphInput, answerTop10patients)
    //compute Jaccard coefficient on the graph 
    println("the top 10 most similar patients are: ")
    // print the patinet IDs here
    answerTop10patients.foreach(println)
    println("the top 10 meds, diagnoses, and labs for these 10 patients are: ")
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
    */
    
    def toInt(s: String):Int = 
  {
    try 
    {
        s.toInt
    } 
    catch 
    {
        case e:Exception => 0
    }
  }

  def toFloat(s: String):Float = 
  {
    try 
    {
        s.toFloat
    } 
    catch 
    {
        case e:Exception => 0
    }
  }
  
  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientProperty], RDD[Medication], RDD[Observation], RDD[Diagnostic]) = {

    // split / clean data
    val patient_data = CSVUtils.loadCSVAsTable(sqlContext, "data/person.csv", "patient")
    val patients = patient_data.map(p=> PatientProperty(toInt(p(0).toString), toInt(p(1).toString), toInt(p(2).toString), toInt(p(3).toString), toInt(p(4).toString), toInt(p(5).toString), toInt(p(6).toString), toInt(p(7).toString), toInt(p(8).toString), toInt(p(9).toString), p(10).toString, p(11).toString, p(12).toString, p(13).toString))
    println("Patients", patients.count)
    
    val diagnostics_data = CSVUtils.loadCSVAsTable(sqlContext, "data/condition_occurrence.csv", "diagnostic")
    val diagnostics = diagnostics_data.map(a => Diagnostic(toInt(a(0).toString), toInt(a(1).toString), toInt(a(2).toString), a(3).toString, a(4).toString, toInt(a(5).toString), a(6).toString, toInt(a(7).toString), toInt(a(8).toString), a(9).toString))
    println("Diagnostics", diagnostics.count)
    
    val lab_data = CSVUtils.loadCSVAsTable(sqlContext, "data/observation.csv", "lab")
    val labResults = lab_data.map(l => Observation(toInt(l(0).toString), toInt(l(1).toString), toInt(l(2).toString), l(3).toString, l(4).toString, toFloat(l(5).toString), l(6).toString, toInt(l(7).toString), toInt(l(8).toString), toFloat(l(9).toString), toFloat(l(10).toString), toInt(l(11).toString), toInt(l(12).toString), toInt(l(13).toString), toInt(l(14).toString), l(15).toString, l(16).toString))
    println("labResults", labResults.count)

    val med_data = CSVUtils.loadCSVAsTable(sqlContext, "data/drug_exposure.csv", "medication")
    val medication = med_data.map(p => Medication(toInt(p(0).toString), toInt(p(1).toString), toInt(p(2).toString), p(3).toString, p(4).toString, toInt(p(5).toString), p(6).toString, toInt(p(7).toString), toInt(p(8).toString), toInt(p(9).toString), p(10).toString, toInt(p(11).toString), toInt(p(12).toString),toInt(p(13).toString), p(14).toString))
    println("medication", medication.count)
    /*
    val rxnorm_data = CSVUtils.loadCSVAsTable(sqlContext, "data/rxnorm.csv", "rxnorm")
    val rxnorm = rxnorm_data.map(r => (r(0).toString.toInt, r(1).toString, r(5).toString))
    println("rxnorm", rxnorm.count)

    val loinc_data = CSVUtils.loadCSVAsTable(sqlContext, "data/loinc.csv", "loinc")
    val loinc = loinc_data.map(l => (l(0).toString.toInt, l(1).toString, l(5).toString))
    println("loinc", loinc.count)
    */

    val snomed_data = CSVUtils.loadCSVAsTable(sqlContext, "data/snomed.csv", "snomed")
    val snomed = snomed_data.map(s => Snomed(s(0).toString.toInt, s(1).toString, s(5).toString))
    println("snomed", snomed.count)

    val ancestor_data = CSVUtils.loadCSVAsTable(sqlContext, "data/ancestors.csv", "ancestors")
    val ancestors = ancestor_data.map(s => ConceptAncestor(s(0).toString.toInt, s(1).toString.toInt))
    println("ancestors", ancestors.count)

    (null, null, null, null)
    //(patients, medication, labResults, diagnostics)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
