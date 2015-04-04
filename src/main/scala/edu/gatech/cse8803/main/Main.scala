package edu.gatech.cse8803.main

import edu.gatech.cse8803.graphconstruct.GraphLoader
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.metrics._
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.randomwalk._
import edu.gatech.cse8803.main._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}
import org.postgresql.Driver
import scala.io.Source

import scala.collection.mutable.MutableList


object Main {

    def main(args: Array[String]) {
        val sc = createContext
        val sqlContext = new SQLContext(sc)

        /** get configuration*/
        val conf = ConfigFactory.load()

        /** initialize loading of data */
        //loadRddRawData2(sqlContext, conf);
        val (patient, medication, labResult, diagnostic) = loadRddRawData(sc, sqlContext, conf)

        val (rxnorm, loinc, snomed, snomed_ancestors, rxnorm_ancestors, snomed_relations, rxnorm_relations, loinc_relations) = loadRddRawDataVocab(sc, sqlContext, conf)

        /** build the graph */
        val graph = GraphLoader.load(patient, medication, labResult, diagnostic, rxnorm, loinc, snomed, snomed_ancestors, rxnorm_ancestors, snomed_relations, rxnorm_relations, loinc_relations)
        
        //compute pagerank
        testKNN(graph)
        
        //Jaccard using only diagnosis
        //testCosine(graph, 1, 0, 0)
        
        //Weighted Jaccard
        //testJaccard(graph, 0.5, 0.3, 0.2)

        //Random walk similarity
        //testRandomWalk(graph)
        
        //testCosine(graph, 1, 0, 0)
    }
  
  def testCosine( graphInput:  Graph[VertexProperty, EdgeProperty], wd: Double, wm: Double, wl: Double ) = {
    val patientIDtoLookup = "-87907000001"

    val answerTop10patients = CosineSimilarity.cosineSimilarityOneVsAll(graphInput, patientIDtoLookup, wd, wm, wl)
    answerTop10patients.foreach(println)
    null
  }
  
  def testKNN( graphInput:  Graph[VertexProperty, EdgeProperty] ) = {
    
    //val patientIDtoLookup = "-87907000001"
    //val patientIDtoLookup = "-94169102" //dead
    //val knnanswer = KNN.knnAllVsAll(graphInput, patientIDtoLookup)
    val knnanswer = graphInput.vertices.filter(t=>(t._1<0)).map(x => (x._1, x._2.asInstanceOf[PatientProperty].dead, KNN.knnAllVsAll(graphInput, x._1.toString)))
    knnanswer.repartition(1).saveAsTextFile("knn.txt")
    //println("KNN answer", knnanswer)
  }
  
  def testJaccard( graphInput:  Graph[VertexProperty, EdgeProperty], wd: Double, wm: Double, wl: Double ) = {
    val patientIDtoLookup = "-87907000001"

    val answerTop10patients = Jaccard.jaccardSimilarityOneVsAll(graphInput, patientIDtoLookup, wd, wm, wl)
    println("Jaccard values")
    answerTop10patients.foreach(println)
    /*val (answerTop10med, answerTop10diag, answerTop10lab) = Jaccard.summarize(graphInput, answerTop10patients)
    //compute Jaccard coefficient on the graph 
    println("the top 10 most similar patients are: ")
    // print the patinet IDs here
    println("the top 10 meds, diagnoses, and labs for these 10 patients are: ")
    //print the meds, diagnoses and labs here
    answerTop10med.foreach(println)
    answerTop10diag.foreach(println)
    answerTop10lab.foreach(println)*/
    null
  }
  
  def testRandomWalk( graphInput:  Graph[VertexProperty, EdgeProperty] ) = {
    val patientIDtoLookup = "-87907000001"
    val answerTop10patients = RandomWalk.randomWalkOneVsAll(graphInput, patientIDtoLookup)
    println("Random walk values")
    answerTop10patients.foreach(println)
    /*val (answerTop10med, answerTop10diag, answerTop10lab) = RandomWalk.summarize(graphInput, answerTop10patients)
    /* compute Jaccard coefficient on the graph */
    println("the top 10 most similar patients are: ")
    // print the patinet IDs here
    answerTop10patients.foreach(println)
    println("the top 10 meds, diagnoes, and labs for these 10 patients are: ")
    //print the meds, diagnoses and labs here
    answerTop10med.foreach(println)
    answerTop10diag.foreach(println)
    answerTop10lab.foreach(println)*/
    null
  }


  def testPageRank( graphInput:  Graph[VertexProperty, EdgeProperty] ) = {
    //run pagerank provided by GraphX
    //print the top 5 mostly highly ranked vertices
    //for each vertex print the vertex name, which can be patientID, test_name or medication name and the corresponding rank
    val p = GraphLoader.runPageRank(graphInput)
    p.foreach(println)
  }

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
    
    def loadRddRawData2(sqlContext: SQLContext, conf:Config) = {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        /*
        val rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM person;")
        rs.next()
        val patient_count= rs.getInt("cnt")
        
        Class.forName("org.postgresql.Driver").newInstance()
        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        DriverManager.getConnection(conn_str)


        val patients = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT * from person ORDER BY person.person_id OFFSET ? LIMIT ?;",0,patient_count,1, r => r.getLong("person_id"))
        */
        val rrs = stmt.executeQuery("select COUNT(*) as cnt from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_1 != c.concept_id_2;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")
        
        Class.forName("org.postgresql.Driver").newInstance()
        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        //DriverManager.getConnection(conn_str)

        val rxnorm_relations = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "select c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_1 != c.concept_id_2 ORDER BY concept_relationship.concept_id_1, concept_relationship.concept_id_2 OFFSET ? LIMIT ?;",
            0, rrs_count,1
            ,r=> (rrs.getInt("concept_id_1"), rrs.getInt("concept_id_2"), rrs.getString("relationship_name")))
        
        
        println("rxnorm R", rxnorm_relations.count)
        //println("Patients: ", patients.count)
    }

    def loadRddRawData(sc: SparkContext,sqlContext: SQLContext, conf:Config): (RDD[PatientProperty], RDD[Medication], RDD[Observation], RDD[Diagnostic]) = {

        // connect to datasource
        val connection = Datasource.connectServer(conf, conf.getString("db-setting.database"))
        val stmt = connection.getConnection.createStatement()
        
        // load Tables

        //Person Table
        val rs = stmt.executeQuery("SELECT p.*, d.death_date FROM person as p left join death as d on p.person_id = d.person_id;")
        val person: MutableList[PatientProperty] = MutableList()
        while (rs.next()) 
        {
            person ++= MutableList(PatientProperty(rs.getLong("person_id"), rs.getInt("gender_concept_id"), rs.getInt("year_of_birth"), rs.getInt("month_of_birth"), rs.getInt("day_of_birth"), rs.getInt("race_concept_id"), rs.getInt("ethnicity_concept_id"), rs.getInt("location_id"), rs.getInt("provider_id"), rs.getInt("care_site_id"), rs.getString("person_source_value"), rs.getString("gender_source_value"), rs.getString("race_source_value"), rs.getString("ethnicity_source_value"), if(rs.getString("death_date")!=null) 1 else 0 ))
        }
        val patients = sc.parallelize(person)
        //println("Patients", patients.count)
        
        val patientVertices: RDD[(VertexId, VertexProperty)] = patients.map(p => ((-p.person_id).toLong, p))
    
        //Diagnostic
        val ds = stmt.executeQuery("SELECT * FROM condition_occurrence;")
        val diagnosis: MutableList[Diagnostic] = MutableList()
        while (ds.next()) 
        {
            diagnosis ++= MutableList(Diagnostic(ds.getInt("condition_occurrence_id"), ds.getLong("person_id"), ds.getInt("condition_concept_id"), ds.getString("condition_start_date"), ds.getString("condition_end_date"), ds.getInt("condition_type_concept_id"), ds.getString("stop_reason"), ds.getInt("associated_provider_id"), ds.getBigDecimal("visit_occurrence_id"), ds.getString("condition_source_value")))
        }
        val diagnostics = sc.parallelize(diagnosis)
        //println("Diagnostics", diagnostics.count)
        
        //Medications
        val ms = stmt.executeQuery("SELECT * FROM drug_exposure;")
        val medicines: MutableList[Medication] = MutableList()
        while (ms.next()) 
        {
            medicines ++= MutableList(Medication(ms.getInt("drug_exposure_id"), ms.getLong("person_id"), ms.getInt("drug_concept_id"), ms.getString("drug_exposure_start_date"), ms.getString("drug_exposure_end_date"), ms.getInt("drug_type_concept_id"), ms.getString("stop_reason"), ms.getInt("refills"), ms.getInt("quantity"), ms.getInt("days_supply"), ms.getString("sig"), ms.getInt("prescribing_provider_id"), ms.getBigDecimal("visit_occurrence_id"), ms.getInt("relevant_condition_concept_id"), ms.getString("drug_source_value")))
        }
        val medication = sc.parallelize(medicines)
        //println("medication", medication.count)
        
        //Labresults
        val ls = stmt.executeQuery("SELECT * FROM observation;")
        val labs: MutableList[Observation] = MutableList()
        while (ls.next()) 
        {
            labs ++= MutableList(Observation(ls.getInt("observation_id"), ls.getLong("person_id"), ls.getInt("observation_concept_id"), ls.getString("observation_date"), ls.getString("observation_time"), ls.getFloat("value_as_number"), ls.getString("value_as_string"), ls.getInt("value_as_concept_id"), ls.getInt("unit_concept_id"), ls.getFloat("range_low"), ls.getFloat("range_high"), ls.getInt("observation_type_concept_id"), ls.getInt("associated_provider_id"), ls.getBigDecimal("visit_occurrence_id"), ls.getInt("relevant_condition_concept_id"), ls.getString("observation_source_value"), ls.getString("units_source_value")))
        }
        val labResults = sc.parallelize(labs)
        //println("labResults", labResults.count)
        
        //val labResults =null

        (patients, medication, labResults, diagnostics)
    }   

    def loadRddRawDataVocab(sc: SparkContext,sqlContext: SQLContext, conf:Config): (RDD[Vocabulary], RDD[Vocabulary], RDD[Vocabulary], RDD[ConceptAncestor], RDD[ConceptAncestor], RDD[ConceptRelation], RDD[ConceptRelation], RDD[ConceptRelation]) = {
        val v_connection = Datasource.connectServer(conf, conf.getString("db-setting.database_vocab"))
        val v_stmt = v_connection.getConnection.createStatement()

        //RxNorm
        val rds = v_stmt.executeQuery("SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 8;")
        val rxnorm_data: MutableList[Vocabulary] = MutableList()
        while (rds.next()) 
        {
            rxnorm_data += Vocabulary(rds.getInt("concept_id"), rds.getString("concept_name"), rds.getString("concept_code"))
        }
        val rxnorm = sc.parallelize(rxnorm_data)
        println("rxnorm", rxnorm.count)

        val ras = v_stmt.executeQuery("SELECT ancestor_concept_id, descendant_concept_id FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 8) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 8) AND descendant_concept_id != ancestor_concept_id;")
        val rxnorm_ancestor_data: MutableList[ConceptAncestor] = MutableList()
        while (ras.next()) 
        {
            rxnorm_ancestor_data += ConceptAncestor(ras.getInt("ancestor_concept_id"), ras.getInt("descendant_concept_id"))
        }
        val rxnorm_ancestors = sc.parallelize(rxnorm_ancestor_data)
        println("rxnorm A", rxnorm_ancestors.count)
    
        val rrs = v_stmt.executeQuery("select c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_1 != c.concept_id_2;")
        val rxnorm_relation_data: MutableList[ConceptRelation] = MutableList()
        while (rrs.next()) 
        {
            rxnorm_relation_data += ConceptRelation(rrs.getInt("concept_id_1"), rrs.getInt("concept_id_2"), rrs.getString("relationship_name"))
        }
        val rxnorm_relations = sc.parallelize(rxnorm_relation_data)
        println("rxnorm R", rxnorm_relations.count)
        
        //Loinc
        val lds = v_stmt.executeQuery("SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 6;")
        val loinc_data: MutableList[Vocabulary] = MutableList()
        while (lds.next()) 
        {
            loinc_data += Vocabulary(lds.getInt("concept_id"), lds.getString("concept_name"), lds.getString("concept_code"))
        }
        val loinc = sc.parallelize(loinc_data)
        println("loinc", loinc.count)

        
        val lrs = v_stmt.executeQuery("select c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 6) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 6) and c.concept_id_1 != c.concept_id_2;")
        val loinc_relation_data: MutableList[ConceptRelation] = MutableList()
        while (lrs.next()) 
        {
            loinc_relation_data += ConceptRelation(lrs.getInt("concept_id_1"), lrs.getInt("concept_id_2"), lrs.getString("relationship_name"))
        }
        val loinc_relations = sc.parallelize(loinc_relation_data)
        println("loinc R", loinc_relations.count)
        
        //Snomed
        val sds = v_stmt.executeQuery("SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 1;")
        val snomed_data: MutableList[Vocabulary] = MutableList()
        while (sds.next()) 
        {
            snomed_data += Vocabulary(sds.getInt("concept_id"), sds.getString("concept_name"), sds.getString("concept_code"))
        }
        val snomed = sc.parallelize(snomed_data)
        println("Snomed", snomed.count)
        
        val sas = v_stmt.executeQuery("SELECT ancestor_concept_id, descendant_concept_id FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 1) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 1) AND descendant_concept_id != ancestor_concept_id;")
        val snomed_ancestor_data: MutableList[ConceptAncestor] = MutableList()
        while (sas.next()) 
        {
            snomed_ancestor_data += ConceptAncestor(sas.getInt("ancestor_concept_id"), sas.getInt("descendant_concept_id"))
        }
        val snomed_ancestors = sc.parallelize(snomed_ancestor_data)
        println("Snomed A", snomed_ancestors.count)

        val srs = v_stmt.executeQuery("select c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 1) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 1) and c.concept_id_1 != c.concept_id_2;")
        val snomed_relation_data: MutableList[ConceptRelation] = MutableList()
        while (srs.next()) 
        {
            snomed_relation_data += ConceptRelation(srs.getInt("concept_id_1"), srs.getInt("concept_id_2"), srs.getString("relationship_name"))
        }
        val snomed_relations = sc.parallelize(snomed_relation_data)
        println("Snomed R", snomed_relations.count)
        /*
        val snomed_ancestors=null
        val rxnorm_ancestors=null
        val snomed_relations=null
        val rxnorm_relations=null
        val loinc_relations=null*/
        (rxnorm, loinc, snomed,snomed_ancestors, rxnorm_ancestors, snomed_relations, rxnorm_relations, loinc_relations)
    }

    def createContext(appName: String, masterUrl: String): SparkContext = {
        val conf = new SparkConf().setAppName(appName).setMaster(masterUrl).set("spark.driver.memory", "2g").set("spark.executor.memory", "2g")
        new SparkContext(conf)
    }

    def createContext(appName: String): SparkContext = createContext(appName, "local")

    def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
}
