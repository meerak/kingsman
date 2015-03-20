package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.graphconstruct.GraphLoader
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.jaccard._
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.randomwalk._
import edu.gatech.cse8803.main._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{ConfigFactory, Config}
import scala.io.Source

import scala.collection.mutable.MutableList

object Main {

  def main(args: Array[String]) {
    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** get configuration*/
    val conf = ConfigFactory.load()

    /** initialize loading of data */
   
    //build the graph
    val (patient, medication, labResult, diagnostic, rxnorm, loinc, snomed, snomed_ancestors, rxnorm_ancestors, snomed_relations, rxnorm_relations, loinc_relations) = loadRddRawData(sc, sqlContext, conf)

    //build the graph
    val graph = GraphLoader.load(patient, medication, labResult, diagnostic, rxnorm, loinc, snomed, snomed_ancestors, rxnorm_ancestors, snomed_relations, rxnorm_relations, loinc_relations)
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
 /*   
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
  }*/
  
  def loadRddRawData(sc: SparkContext, sqlContext: SQLContext, conf:Config): (RDD[PatientProperty], RDD[Medication], RDD[Observation], RDD[Diagnostic], RDD[Vocabulary], RDD[Vocabulary], RDD[Vocabulary], RDD[ConceptAncestor], RDD[ConceptAncestor], RDD[ConceptRelation], RDD[ConceptRelation], RDD[ConceptRelation]) = {

    val connection = Datasource.connectServer(conf, "omop_v4_mimic2")
    val stmt = connection.getConnection.createStatement()

    //Person Table
    val rs = stmt.executeQuery("SELECT * FROM person;")
    val person: MutableList[PatientProperty] = MutableList()
    while (rs.next()) 
    {
        person ++= MutableList(PatientProperty(rs.getInt("person_id"), rs.getInt("gender_concept_id"), rs.getInt("year_of_birth"), rs.getInt("month_of_birth"), rs.getInt("day_of_birth"), rs.getInt("race_concept_id"), rs.getInt("ethnicity_concept_id"), rs.getInt("location_id"), rs.getInt("provider_id"), rs.getInt("care_site_id"), rs.getString("person_source_value"), rs.getString("gender_source_value"), rs.getString("race_source_value"), rs.getString("ethnicity_source_value")))
    }
    val patients = sc.parallelize(person)
    //println("Patients", patients.count)
    
    //Diagnostic
    val ds = stmt.executeQuery("SELECT * FROM condition_occurrence;")
    val diagnosis: MutableList[Diagnostic] = MutableList()
    while (ds.next()) 
    {
        diagnosis ++= MutableList(Diagnostic(ds.getInt("condition_occurrence_id"), ds.getInt("person_id"), ds.getInt("condition_concept_id"), ds.getString("condition_start_date"), ds.getString("condition_end_date"), ds.getInt("condition_type_concept_id"), ds.getString("stop_reason"), ds.getInt("associated_provider_id"), ds.getInt("visit_occurrence_id"), ds.getString("condition_source_value")))
    }
    val diagnostics = sc.parallelize(diagnosis)
    //println("Diagnostics", diagnostics.count)
    
    //Medications
    val ms = stmt.executeQuery("SELECT * FROM drug_exposure;")
    val medicines: MutableList[Medication] = MutableList()
    while (ms.next()) 
    {
        medicines ++= MutableList(Medication(ms.getInt("drug_exposure_id"), ms.getInt("person_id"), ms.getInt("drug_concept_id"), ms.getString("drug_exposure_start_date"), ms.getString("drug_exposure_end_date"), ms.getInt("drug_type_concept_id"), ms.getString("stop_reason"), ms.getInt("refills"), ms.getInt("quantity"), ms.getInt("days_supply"), ms.getString("sig"), ms.getInt("prescribing_provider_id"), ms.getInt("visit_occurrence_id"), ms.getInt("relevant_condition_concept_id"), ms.getString("drug_source_value")))
    }
    val medication = sc.parallelize(medicines)
    //println("medication", medication.count)
    
    //Labresults
    val ls = stmt.executeQuery("SELECT * FROM observation;")
    val labs: MutableList[Observation] = MutableList()
    while (ls.next()) 
    {
        labs ++= MutableList(Observation(ls.getInt("observation_id"), ls.getInt("person_id"), ls.getInt("observation_concept_id"), ls.getString("observation_date"), ls.getString("observation_time"), ls.getFloat("value_as_number"), ls.getString("value_as_string"), ls.getInt("value_as_concept_id"), ls.getInt("unit_concept_id"), ls.getFloat("range_low"), ls.getFloat("range_high"), ls.getInt("observation_type_concept_id"), ls.getInt("associated_provider_id"), ls.getInt("visit_occurrence_id"), ls.getInt("relevant_condition_concept_id"), ls.getString("observation_source_value"), ls.getString("units_source_value")))
    }
    val labResults = sc.parallelize(labs)
    println("labResults", labResults.count)

    val v_connection = Datasource.connectServer(conf, "omop_vocabulary_v4")
    val v_stmt = v_connection.getConnection.createStatement()

    //RxNorm
    val vrs = v_stmt.executeQuery("SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 8;")
    val rxnorm_data: MutableList[Vocabulary] = MutableList()
    while (vrs.next()) 
    {
        rxnorm_data ++= MutableList(Vocabulary(vrs.getInt("concept_id"), vrs.getString("concept_name"), vrs.getString("concept_code")))
    }
    val rxnorm = sc.parallelize(rxnorm_data)
    println("rxnorm", rxnorm.count)

    /*
    val rxnorm_ancestor_data = CSVUtils.loadCSVAsTable(sqlContext, "data/ancestor_rxnorm.csv", "ancestors_rxnorm")
    val rxnorm_ancestors = rxnorm_ancestor_data.map(s => ConceptAncestor(s(0).toString.toInt, s(1).toString.toInt))
    
    val loinc_data = CSVUtils.loadCSVAsTable(sqlContext, "data/loinc.csv", "loinc")
    val loinc = loinc_data.map(l => Vocabulary(l(0).toString.toInt, l(1).toString, l(5).toString))
    //println("loinc", loinc.count)

    val snomed_data = CSVUtils.loadCSVAsTable(sqlContext, "data/snomed.csv", "snomed")
    val snomed = snomed_data.map(s => Vocabulary(s(0).toString.toInt, s(1).toString, s(5).toString))
    //println("snomed", snomed.count)

    val ancestor_data = CSVUtils.loadCSVAsTable(sqlContext, "data/ancestor_snomed.csv", "ancestors")
    val snomed_ancestors = ancestor_data.map(s => ConceptAncestor(s(0).toString.toInt, s(1).toString.toInt))

    val snomed_relation_data = CSVUtils.loadCSVAsTable(sqlContext, "data/relationship_snomed.csv", "relationship_snomed")
    val snomed_relations = snomed_relation_data.map(s => ConceptRelation(s(0).toString.toInt, s(1).toString.toInt, s(11).toString))

    val rxnorm_relation_data = CSVUtils.loadCSVAsTable(sqlContext, "data/relationship_snomed.csv", "relationship_rxnorm")
    val rxnorm_relations = rxnorm_relation_data.map(s => ConceptRelation(s(0).toString.toInt, s(1).toString.toInt, s(11).toString))

    val loinc_relation_data = CSVUtils.loadCSVAsTable(sqlContext, "data/relationship_snomed.csv", "relationship_loinc")
    val loinc_relations = loinc_relation_data.map(s => ConceptRelation(s(0).toString.toInt, s(1).toString.toInt, s(11).toString))
    //println("ancestors", ancestors.count)
    
    (patients, medication, labResults, diagnostics, rxnorm, loinc, snomed,snomed_ancestors, rxnorm_ancestors, snomed_relations, rxnorm_relations, loinc_relations)
    */
    (null, null, null, null, null, null, null, null, null, null, null, null)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
