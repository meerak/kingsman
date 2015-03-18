package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import edu.gatech.cse8803.enums._

object GraphLoader {
  def load(patients: RDD[PatientProperty],

    medications: RDD[Medication], labResults:RDD[Observation], diagnostics: RDD[Diagnostic], rxnorm:RDD[Vocabulary], loinc: RDD[Vocabulary], snomed:RDD[Vocabulary], snomed_ancestors:RDD[ConceptAncestor], rxnorm_ancestors:RDD[ConceptAncestor]): Graph[VertexProperty, EdgeProperty] = {

    //val sqlContext = new org.apache.spark.sql.SQLContext(patients.sparkContext)
    //val sc = sqlContext.sparkContext
    val loincVertices: RDD[(VertexId, VertexProperty)] = loinc.map(a=>(a.concept_id.toLong, VocabularyProperty(a.concept_id)))

    val snomedVertices: RDD[(VertexId, VertexProperty)] = snomed.map(a=>(a.concept_id.toLong, VocabularyProperty(a.concept_id)))
    val snomedEdges: RDD[Edge[EdgeProperty]] = snomed_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))

    val rxnormVertices: RDD[(VertexId, VertexProperty)] = rxnorm.map(a=>(a.concept_id.toLong, VocabularyProperty(a.concept_id)))
    val rxnormEdges: RDD[Edge[EdgeProperty]] = rxnorm_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))

    /*val maxPatientID = patients.map(f=>f.patientID).toArray.maxBy(f=>f.toLong).toLong
    
    val labResultsProperty: RDD[VertexProperty] = labResults.map(a=>a.labName).distinct.map(a=>LabResultProperty(a))
    val labCount = labResultsProperty.count.toLong
    val labIndices  = sc.parallelize((maxPatientID +1) to (maxPatientID + labCount))
    val labVertices = labIndices.zip(labResultsProperty)
    
    val prevCount = maxPatientID + labCount
    val medicationsProperty: RDD[VertexProperty] = medications.map(m=>m.medicine).distinct.map(m=>MedicationProperty(m))
    val medicationCount = medicationsProperty.count.toLong
    val medicationIndices  = sc.parallelize((prevCount+1) to (prevCount + medicationCount))
    val medicationVertices = medicationIndices.zip(medicationsProperty)
    val prevCount1 = prevCount + medicationCount
    val diagnosticsProperty: RDD[VertexProperty] = diagnostics.map(d=>d.icd9code).distinct.map(d=>DiagnosticProperty(d))
    val diagnosticCount = diagnosticsProperty.count.toInt
    val diagnosticIndices  = sc.parallelize((prevCount1+1) to (prevCount1 + diagnosticCount))
    val diagnosticVertices = diagnosticIndices.zip(diagnosticsProperty)
    
    var vertices = patientVertices.union(labVertices).union(medicationVertices).union(diagnosticVertices)
    
    val maxLabResults = labResults.groupBy(x => (x.patientID, x.labName)).map(x => x._2.toArray.maxBy(y => y.date)).map(l => (l.labName, (l)))
    val mapLabResults = labVertices.map(l => (l._2.asInstanceOf[LabResultProperty].testName, (l._1)))
    val labEdges:RDD[Edge[EdgeProperty]] = maxLabResults.join(mapLabResults).map(e => Edge(e._2._1.patientID.toLong, e._2._2, PatientLabEdgeProperty(e._2._1)))
    val labEdgesReverse:RDD[Edge[EdgeProperty]] = maxLabResults.join(mapLabResults).map(e => Edge(e._2._2, e._2._1.patientID.toLong, PatientLabEdgeProperty(e._2._1)))
    val maxMedication = medications.groupBy(x => (x.patientID, x.medicine)).map(x => x._2.toArray.maxBy(y => y.date)).map(m => (m.medicine, (m)))
    val mapMedication = medicationVertices.map(m => (m._2.asInstanceOf[MedicationProperty].medicine, (m._1)))
    val medicationEdges:RDD[Edge[EdgeProperty]] = maxMedication.join(mapMedication).map(e => Edge(e._2._1.patientID.toLong, e._2._2, PatientMedicationEdgeProperty(e._2._1)))
    val medicationEdgesReverse:RDD[Edge[EdgeProperty]] = maxMedication.join(mapMedication).map(e => Edge(e._2._2, e._2._1.patientID.toLong, PatientMedicationEdgeProperty(e._2._1)))
    val maxDiagnostics = diagnostics.groupBy(x => (x.patientID, x.icd9code)).map(x => x._2.toArray.maxBy(y => y.date)).map(d=> (d.icd9code, (d)))
    val mapDiagnostics = diagnosticVertices.map(v => (v._2.asInstanceOf[DiagnosticProperty].icd9code, (v._1)))
    val diagnosticEdges:RDD[Edge[EdgeProperty]] = maxDiagnostics.join(mapDiagnostics).map(e => Edge(e._2._1.patientID.toLong, e._2._2, PatientDiagnosticEdgeProperty(e._2._1)))
    val diagnosticEdgesReverse:RDD[Edge[EdgeProperty]] = maxDiagnostics.join(mapDiagnostics).map(e => Edge(e._2._2, e._2._1.patientID.toLong, PatientDiagnosticEdgeProperty(e._2._1)))
    val edges = labEdges.union(medicationEdges).union(diagnosticEdges).union(labEdgesReverse).union(medicationEdgesReverse).union(diagnosticEdgesReverse)*/
    val vertices = snomedVertices.union(rxnormVertices).union(loincVertices)
    val edges = snomedEdges.union(rxnormEdges)
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)
    println("all vertices: ", graph.vertices.count)
    println("all edges: ", graph.edges.count)
    //edges.repartition(1).saveAsTextFile("EdgesBidirectional")
    graph
  }
  /*
  def runPageRank(graph:  Graph[VertexProperty, EdgeProperty] ): List[(String, Double)] ={
    //run pagerank provided by GraphX
    //return the top 5 mostly highly ranked vertices
    //for each vertex return the vertex name, which can be patientID, test_name or medication name and the corresponding rank
    //see example below
    
    val prGraph = graph.staticPageRank(10, 0.15).cache
    val prGraphProp = graph.outerJoinVertices(prGraph.vertices) {
        (id, VertexProperty, rank) => (VertexProperty match{ 
        case a: PatientProperty => a.patientID
        case b: MedicationProperty => b.medicine
        case c: LabResultProperty => c.testName
        case d: DiagnosticProperty => d.icd9code
        },  rank.getOrElse(0.0))
    }
    val  top = prGraphProp.vertices.top(5) {
        Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2)
    }

    val p  = top.map(t=> (t._2._1 , t._2._2)).toList
    p
  }*/
}
