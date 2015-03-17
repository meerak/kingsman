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
    val (patient, medication, diagnostic, snomed, ancestors) = loadRddRawData(sqlContext)

    //build the graph
    val graph = GraphLoader.load( patient, medication, diagnostic, snomed, ancestors)

    /*
    //compute pagerank
    testPageRank(graph)
    
    //Jaccard using only diagnosis
    testJaccard(graph, 1, 0, 0)

    //Weighted Jaccard
    testJaccard(graph, 0.5, 0.3, 0.2)

    //Random walk similarity
    testRandomWalk(graph)*/
  }

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
    //val p = GraphLoader.runPageRank(graphInput)
    //p.foreach(println)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[PatientProperty], RDD[Medication], RDD[Observation], RDD[Diagnostic], RDD[Snomed], RDD[ConceptAncestor]) = {

    // split / clean data
    val patient_data = CSVUtils.loadCSVAsTable(sqlContext, "data/person.csv", "patient")
    val patients = patient_data.map(p=> PatientProperty(p(0).toString, p(1).toString, p(2).toString, p(3).toString))
    println("Patients", patients.count)

    val diagnostics_data = CSVUtils.loadCSVAsTable(sqlContext, "data/condition_occurrence.csv", "diagnostic")
    val diagnostics = diagnostics_data.map(a => Diagnostic(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toString, a(4).toString, a(5).toInt, a(6).toString, a(7).toInt, a(8).toInt, a(9).toString))
    println("Diagnostics", diagnostics.count)
    
    /*val lab_data = CSVUtils.loadCSVAsTable(sqlContext, "data/observation.csv", "lab")
    val labResults = lab_data.map(l => Observation(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toString, l(4).toString, l(5).toFloat, l(6).toString, l(7).toInt, l(8).toInt, l(9).toFloat, l(10).toFloat, l(11).toInt, l(12).toInt, l(13).toInt, l(14).toInt, l(15).toString, l(16).toString))
    println("labResults", labResults.count)*/

    val med_data = CSVUtils.loadCSVAsTable(sqlContext, "data/drug_exposure.csv", "medication")
    val medication = med_data.map(p => Medication(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toString, p(4).toString, p(5).toInt, p(6).toString, p(7).toInt, p(8).toInt, p(9).toInt, p(10).toString, p(11).toInt, p(12).toInt, p(13).toInt, p(14).toString))
    println("medication", medication.count)

    val rxnorm_data = CSVUtils.loadCSVAsTable(sqlContext, "data/rxnorm.csv", "rxnorm")
    val rxnorm = rxnorm_data.map(r => (r(0).toInt, r(1).toString, r(5).toString))
    println("rxnorm", rxnorm.count)

    val loinc_data = CSVUtils.loadCSVAsTable(sqlContext, "data/loinc.csv", "loinc")
    val loinc = loinc_data.map(l => (l(0).toInt, l(1).toString, l(5).toString))
    println("loinc", loinc.count)

    val snomed_data = CSVUtils.loadCSVAsTable(sqlContext, "data/snomed.csv", "snomed")
    val snomed = snomed_data.map(s => Snomed(s(0).toInt, s(1).toString, s(5).toString))
    println("snomed", snomed.count)

    val snomed_ancestor_data = CSVUtils.loadCSVAsTable(sqlContext, "data/ancestors.csv", "ancestors")
    val ancestors = snomed_ancestor_data.map(s => ConceptAncestor(s(0).toInt, s(1).toInt, s(2).toInt, s(3).toInt))
    //println("ancestors", ancestors.count)

    (patients, medication, diagnostics, snomed, ancestors)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
