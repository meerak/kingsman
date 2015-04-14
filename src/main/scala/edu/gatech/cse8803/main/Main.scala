package spark.jobserver

import edu.gatech.cse8803.graphconstruct.GraphLoader
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.metrics._
import edu.gatech.cse8803.model._
import edu.gatech.cse8803.randomwalk._
import edu.gatech.cse8803.main._

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}
import org.postgresql.Driver
import scala.io.Source
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.util.Try

object Main extends SparkJob with NamedRddSupport {

    def loadRddRawDataLabResults(sqlContext: SQLContext, conf:Config) = {
        val dbname = conf.getString("db-setting.database")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("select COUNT(*) as cnt from OBSERVATION")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")
        
        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")

        val labResults = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT * FROM observation where ? <= observation_id and observation_id <+ ?",
            0, rrs_count,10
            ,r=> (Observation(r.getInt("observation_id"), r.getLong("person_id"), r.getInt("observation_concept_id"), r.getString("observation_date"), r.getString("observation_time"), r.getFloat("value_as_number"), r.getString("value_as_string"), r.getInt("value_as_concept_id"), r.getInt("unit_concept_id"), r.getFloat("range_low"), r.getFloat("range_high"), r.getInt("observation_type_concept_id"), r.getInt("associated_provider_id"), 0, r.getInt("relevant_condition_concept_id"), r.getString("observation_source_value"), r.getString("units_source_value"))))
        
        connection.close()
        labResults
    }

    def loadRddRawDataPatients(sqlContext: SQLContext, conf:Config) = {
        val dbname = conf.getString("db-setting.database")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("select MAX(p.person_id) as cnt from person as p left join death as d on p.person_id = d.person_id;")
        rrs.next()
        val rrs_count= rrs.getLong("cnt")
        
        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")

        val patients = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT p.*, d.death_date FROM person as p left join death as d on p.person_id = d.person_id where ? <= p.person_id and p.person_id <= ?;",
            0, rrs_count,10
            ,rs=> (PatientProperty(rs.getLong("person_id"), rs.getInt("gender_concept_id"), rs.getInt("year_of_birth"), rs.getInt("month_of_birth"), rs.getInt("day_of_birth"), rs.getInt("race_concept_id"), rs.getInt("ethnicity_concept_id"), rs.getInt("location_id"), rs.getInt("provider_id"), rs.getInt("care_site_id"), rs.getString("person_source_value"), rs.getString("gender_source_value"), rs.getString("race_source_value"), rs.getString("ethnicity_source_value"), if(rs.getString("death_date")!=null) 1 else 0 )))
        
        connection.close()
        patients
    }

    def loadRddRawDataDiagnostics(sqlContext: SQLContext, conf:Config) = {
         val dbname = conf.getString("db-setting.database")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("select MAX(condition_occurrence_id) as cnt from condition_occurrence")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")
        
        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")

        val diagnostics = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT * FROM condition_occurrence where ? <= condition_occurrence_id and condition_occurrence_id <= ? ;",
            0, rrs_count,10
            ,ds=> (Diagnostic(ds.getInt("condition_occurrence_id"), ds.getLong("person_id"), ds.getInt("condition_concept_id"), ds.getString("condition_start_date"), ds.getString("condition_end_date"), ds.getInt("condition_type_concept_id"), ds.getString("stop_reason"), ds.getInt("associated_provider_id"), ds.getBigDecimal("visit_occurrence_id"), ds.getString("condition_source_value"))))
    
        connection.close()
        diagnostics
    }

    def loadRddRawDataMedication(sqlContext: SQLContext, conf:Config) = {
         val dbname = conf.getString("db-setting.database")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("select MAX(drug_exposure_id) as cnt from drug_exposure;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")
        
        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val medication = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT * from drug_exposure  where ? <= drug_exposure_id and drug_exposure_id <= ?;",
            0, rrs_count,10
            ,ms=> (Medication(ms.getInt("drug_exposure_id"), ms.getLong("person_id"), ms.getInt("drug_concept_id"), ms.getString("drug_exposure_start_date"), ms.getString("drug_exposure_end_date"), ms.getInt("drug_type_concept_id"), ms.getString("stop_reason"), ms.getInt("refills"), ms.getInt("quantity"), ms.getInt("days_supply"), ms.getString("sig"), ms.getInt("prescribing_provider_id"), ms.getBigDecimal("visit_occurrence_id"), ms.getInt("relevant_condition_concept_id"), ms.getString("drug_source_value"))))
        
        connection.close()
        medication
    }

    def loadRddRawDataRxNorm(sqlContext: SQLContext, conf:Config): RDD[Vocabulary] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(concept_id) as cnt FROM concept WHERE vocabulary_id = 8;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val rxnorm = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 8 and ?<=concept_id and concept_id <= ?;",
            0, rrs_count,10
            ,rds => (Vocabulary(rds.getInt("concept_id"), rds.getString("concept_name"), rds.getString("concept_code"))))
        
        connection.close()
        rxnorm
    }

    def loadRddRawDataRxNormAncestor(sqlContext: SQLContext, conf:Config): RDD[ConceptAncestor] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(ancestor_concept_id) as cnt FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 8) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 8) AND descendant_concept_id != ancestor_concept_id;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val rxnorm_ancestor = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT ancestor_concept_id, descendant_concept_id FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 8) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 8) AND descendant_concept_id != ancestor_concept_id AND ?<=ancestor_concept_id and ancestor_concept_id<=?;",
            0, rrs_count, 10
            ,ras => (ConceptAncestor(ras.getInt("ancestor_concept_id"), ras.getInt("descendant_concept_id"))))
        
        connection.close()
        rxnorm_ancestor
    }

    def loadRddRawDataVocabRxnormRelation(sqlContext: SQLContext, conf:Config): RDD[ConceptRelation] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(c.concept_id_1) as cnt FROM concept_relationship as c where c.concept_id_1 in (SELECT concept_id FROM concept WHERE vocabulary_id = 8) and c.concept_id_2 in (SELECT concept_id FROM concept WHERE vocabulary_id = 8) and c.concept_id_1 != c.concept_id_2;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")

        val rxnorm_relations = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
        "SELECT c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 8) and c.concept_id_1 != c.concept_id_2 AND ? <= c.concept_id_1 and c.concept_id_1 <= ?;"
        ,0, rrs_count, 10
        ,ras => (ConceptRelation(ras.getInt("concept_id_1"), ras.getInt("concept_id_2"), ras.getString("relationship_name"))))
        
        connection.close()
        rxnorm_relations
    }

    def loadRddRawDataVocabSnomedRelation(sqlContext: SQLContext, conf:Config): RDD[ConceptRelation] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(c.concept_id_1) as cnt FROM concept_relationship as c where c.concept_id_1 in (SELECT concept_id FROM concept WHERE vocabulary_id = 1) and c.concept_id_2 in (SELECT concept_id FROM concept WHERE vocabulary_id = 1) and c.concept_id_1 != c.concept_id_2;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")

        val snomed_relations = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
        "SELECT c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 1) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 1) and c.concept_id_1 != c.concept_id_2 AND ? <= c.concept_id_1 and c.concept_id_1 <= ?;"
        ,0, rrs_count, 10
        ,ras => (ConceptRelation(ras.getInt("concept_id_1"), ras.getInt("concept_id_2"), ras.getString("relationship_name"))))
        
        connection.close()
        snomed_relations
    }

    def loadRddRawDataVocabLoincRelation(sqlContext: SQLContext, conf:Config): RDD[ConceptRelation] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(c.concept_id_1) as cnt FROM concept_relationship as c where c.concept_id_1 in (SELECT concept_id FROM concept WHERE vocabulary_id = 6) and c.concept_id_2 in (SELECT concept_id FROM concept WHERE vocabulary_id = 6) and c.concept_id_1 != c.concept_id_2;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")

        val loinc_relations = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
        "SELECT c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 6) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 6) and c.concept_id_1 != c.concept_id_2 AND ? <= c.concept_id_1 and c.concept_id_1 <= ?;"
        ,0, rrs_count, 10
        ,ras => (ConceptRelation(ras.getInt("concept_id_1"), ras.getInt("concept_id_2"), ras.getString("relationship_name"))))
        
        connection.close()
        loinc_relations
    }

    def loadRddRawDataVocabRaceRelation(sqlContext: SQLContext, conf:Config): RDD[ConceptRelation] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(c.concept_id_1) as cnt FROM concept_relationship as c where c.concept_id_1 in (SELECT concept_id FROM concept WHERE vocabulary_id = 13) and c.concept_id_2 in (SELECT concept_id FROM concept WHERE vocabulary_id = 13) and c.concept_id_1 != c.concept_id_2;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")

        val race_relations = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
        "SELECT c.concept_id_1 as concept_id_1, c.concept_id_2 as concept_id_2, r.relationship_name as relationship_name from concept_relationship as c join relationship as r on c.relationship_id = r.relationship_id where c.concept_id_1 in (select concept_id from concept where vocabulary_id = 13) and c.concept_id_2 in (select concept_id from concept where vocabulary_id = 13) and c.concept_id_1 != c.concept_id_2 AND ? <= c.concept_id_1 and c.concept_id_1 <= ?;"
        ,0, rrs_count, 10
        ,ras => (ConceptRelation(ras.getInt("concept_id_1"), ras.getInt("concept_id_2"), ras.getString("relationship_name"))))
        
        connection.close()
        race_relations
    }


    def loadRddRawDataSnomed(sqlContext: SQLContext, conf:Config): RDD[Vocabulary] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()

        val rrs = stmt.executeQuery("SELECT MAX(concept_id) as cnt FROM concept WHERE vocabulary_id = 1;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val snomed = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 1 and ?<=concept_id and concept_id<=?;",
            0, rrs_count, 10
            ,rds => (Vocabulary(rds.getInt("concept_id"), rds.getString("concept_name"), rds.getString("concept_code"))))
        
        connection.close()
        snomed
    }

    def loadRddRawDataSnomedAncestor(sqlContext: SQLContext, conf:Config): RDD[ConceptAncestor] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(ancestor_concept_id) as cnt FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 1) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 1) AND descendant_concept_id != ancestor_concept_id;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val snomed_ancestor = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT ancestor_concept_id, descendant_concept_id FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 1) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 1) AND descendant_concept_id != ancestor_concept_id AND ?<=ancestor_concept_id and ancestor_concept_id<=?;",
            0, rrs_count, 10
            ,ras => (ConceptAncestor(ras.getInt("ancestor_concept_id"), ras.getInt("descendant_concept_id"))))
        
        connection.close()
        snomed_ancestor
    }

    def loadRddRawDataLoinc(sqlContext: SQLContext, conf:Config): RDD[Vocabulary] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(concept_id) as cnt FROM concept WHERE vocabulary_id = 6;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val loinc = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 6 and ?<=concept_id and concept_id<=?;",
            0, rrs_count,10
            ,rds => (Vocabulary(rds.getInt("concept_id"), rds.getString("concept_name"), rds.getString("concept_code"))))
        
        connection.close()
        loinc
    }

    def loadRddRawDataRace(sqlContext: SQLContext, conf:Config): RDD[Vocabulary] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(concept_id) as cnt FROM concept WHERE vocabulary_id = 13;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val race = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 13 and ?<=concept_id and concept_id<=?;",
            0, rrs_count, 10
            ,rds => (Vocabulary(rds.getInt("concept_id"), rds.getString("concept_name"), rds.getString("concept_code"))))
        
        connection.close()
        race
    }

    def loadRddRawDataRaceAncestor(sqlContext: SQLContext, conf:Config): RDD[ConceptAncestor] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(ancestor_concept_id) as cnt FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 13) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 13) AND descendant_concept_id != ancestor_concept_id;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val race_ancestor = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT ancestor_concept_id, descendant_concept_id FROM concept_ancestor WHERE ancestor_concept_id IN (select concept_id from concept where vocabulary_id = 13) AND descendant_concept_id IN (select concept_id from concept where vocabulary_id = 13) AND descendant_concept_id != ancestor_concept_id and ?<=ancestor_concept_id and ancestor_concept_id<=?;",
            0, rrs_count, 10
            ,ras => (ConceptAncestor(ras.getInt("ancestor_concept_id"), ras.getInt("descendant_concept_id"))))
        
        connection.close()
        race_ancestor
    }

    def loadRddRawDataGender(sqlContext: SQLContext, conf:Config): RDD[Vocabulary] = 
    {
        val dbname = conf.getString("db-setting.database_vocab")
        
        val connection = Datasource.connectServer(conf, dbname)
        val stmt = connection.getConnection.createStatement()
        
        val rrs = stmt.executeQuery("SELECT MAX(concept_id) as cnt FROM concept WHERE vocabulary_id = 12;")
        rrs.next()
        val rrs_count= rrs.getInt("cnt")

        val conn_str = s"jdbc:postgresql://" + conf.getString("db-setting.host") + ":" +  conf.getString("db-setting.port") + "/" + dbname + "?user=" + conf.getString("db-setting.user") + "&password=" + conf.getString("db-setting.password")
        
        val gender = new JdbcRDD(sqlContext.sparkContext, () => DriverManager.getConnection(conn_str),
            "SELECT concept_id, concept_name, concept_code FROM concept WHERE vocabulary_id = 12 and ?<=concept_id and concept_id<=?;",
            0, rrs_count, 10
            ,rds => (Vocabulary(rds.getInt("concept_id"), rds.getString("concept_name"), rds.getString("concept_code"))))
        
        connection.close()
        gender
    }

    def loadRddRawDataAge(sc: SparkContext): RDD[AgeProperty] =
    {
        val age: RDD[AgeProperty] = sc.parallelize(List(AgeProperty(-10), AgeProperty(-20), AgeProperty(-30), AgeProperty(-40), AgeProperty(-50), AgeProperty(-60), AgeProperty(-70), AgeProperty(-80), AgeProperty(-90), AgeProperty(-100)))
        age
    }

    def createContext(appName: String, masterUrl: String): SparkContext = 
    {
        val conf = new SparkConf().setAppName(appName)
        new SparkContext(conf)
    }

    def createContext(appName: String): SparkContext = createContext(appName, "local")

    def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
    
    override def runJob(sc:SparkContext, config: Config): Any = {
        
        val patientIDtoLookup =  config.getString("input.string")
        val sqlContext = new SQLContext(sc)

        val LOG = LoggerFactory.getLogger(getClass())
        
        val conf = ConfigFactory.load()
        var startTime = System.currentTimeMillis();
        LOG.info("Load data from database into RDD")

        val patient = loadRddRawDataPatients(sqlContext, conf)
        val medication = loadRddRawDataMedication(sqlContext, conf)
        val diagnostic = loadRddRawDataDiagnostics(sqlContext, conf)
        val labResult = loadRddRawDataLabResults(sqlContext, conf)

        val rxnorm = loadRddRawDataRxNorm( sqlContext, conf)
        val rxnorm_ancestors = loadRddRawDataRxNormAncestor( sqlContext, conf)
        val rxnorm_relations = loadRddRawDataVocabRxnormRelation( sqlContext, conf)

        val snomed = loadRddRawDataSnomed(sqlContext, conf)
        val snomed_ancestors = loadRddRawDataSnomedAncestor( sqlContext, conf)
        val snomed_relations = loadRddRawDataVocabSnomedRelation( sqlContext, conf)

        val race = loadRddRawDataRace(sqlContext, conf)
        val race_ancestors = loadRddRawDataRaceAncestor(sqlContext, conf)
        val race_relations = loadRddRawDataVocabRaceRelation( sqlContext, conf)

        val loinc = loadRddRawDataLoinc(sqlContext, conf)
        val loinc_relations = loadRddRawDataVocabLoincRelation( sqlContext, conf)

        val gender = loadRddRawDataGender(sqlContext, conf)

        val age = loadRddRawDataAge(sc)
        var endTime = System.currentTimeMillis()
        println(s"Data loaded in " + (endTime - startTime) +"ms")
    
        val (v,e) = GraphLoader.load(patient, medication, labResult, diagnostic, age, gender, race, rxnorm, loinc, snomed, race_ancestors, snomed_ancestors, rxnorm_ancestors, race_relations, snomed_relations, rxnorm_relations, loinc_relations)

        //val vertices:RDD[(VertexId, VertexProperty)] = graph.vertices
        //val edges:RDD[Edge[EdgeProperty]]  = graph.edges
        namedRdds.getOrElseCreate("vertices", v)
        namedRdds.getOrElseCreate("edges", e)
        
        //namedRdds.getNames.foreach(println)
        null
    }
    override def validate(sc:SparkContext, config: Config): SparkJobValidation = {
        SparkJobValid
    }
}

object SimilarityMin extends SparkJob with NamedRddSupport {

   /* def main(args: Array[String]) {
        val sc = createContext
        runJob(sc, ConfigFactory.parseString(""))
    }*/

    override def runJob(sc:SparkContext, config: Config): Any = {
        
        val LOG = LoggerFactory.getLogger(getClass())
        val patientIDtoLookup =  config.getString("input.string")
        
        val vertices = this.namedRdds.get[(VertexId, VertexProperty)]("vertices").get 
        val edges = this.namedRdds.get[Edge[EdgeProperty]]("edges").get 
        
        var startTime = System.currentTimeMillis();
        val graph = Graph(vertices, edges)
        var endTime = System.currentTimeMillis();
        LOG.info("Graph created in " + (endTime - startTime) +" ms")
        
        startTime = System.currentTimeMillis();
        LOG.info("Started Minimum similarity")
        val answerTop10patients = MinSimilarity.MinSimilarityOneVsAll(graph, patientIDtoLookup, null)
        endTime = System.currentTimeMillis();
        println(s"Minimum similarity calculated in " + (endTime - startTime) +"ms")   

        answerTop10patients//.foreach(println)
       // null
    }
    override def validate(sc:SparkContext, config: Config): SparkJobValidation = {
        Try(config.getString("input.string"))
            .map(x=>SparkJobValid)
            .getOrElse(SparkJobInvalid("No input string"))
    }

     def createContext(appName: String, masterUrl: String): SparkContext = 
    {
        //val conf = new SparkConf().setAppName(appName)
         val conf = new SparkConf().setAppName(appName)
         //.set("spark.driver.memory", "10g").set("spark.executor.memory", "10g")
         //setMaster(masterUrl).
        new SparkContext(conf)
    }

    def createContext(appName: String): SparkContext = createContext(appName, "local")

    def createContext: SparkContext = createContext("CSE 8803 Homework Three Application", "local")
    
}
