package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import edu.gatech.cse8803.enums._

object GraphLoader {
  def load(patients: RDD[PatientProperty], medications: RDD[Medication], labResults:RDD[Observation], diagnostics: RDD[Diagnostic], rxnorm:RDD[Vocabulary], loinc: RDD[Vocabulary], snomed:RDD[Vocabulary], snomed_ancestors:RDD[ConceptAncestor], rxnorm_ancestors:RDD[ConceptAncestor]): Graph[VertexProperty, EdgeProperty] = {

    val patientVertices: RDD[(VertexId, VertexProperty)] = patients.map(p => ((-p.person_id).toLong, p))
    
    val loincVertices: RDD[(VertexId, VertexProperty)] = loinc.map(a=>(a.concept_id.toLong, VocabularyProperty(a.concept_id)))

    val snomedVertices: RDD[(VertexId, VertexProperty)] = snomed.map(a=>(a.concept_id.toLong, VocabularyProperty(a.concept_id)))
    val snomedEdges: RDD[Edge[EdgeProperty]] = snomed_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))

    val rxnormVertices: RDD[(VertexId, VertexProperty)] = rxnorm.map(a=>(a.concept_id.toLong, VocabularyProperty(a.concept_id)))
    val rxnormEdges: RDD[Edge[EdgeProperty]] = rxnorm_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))

    val patlabEdges: RDD[Edge[EdgeProperty]] = labResults.map(x => Edge((-x.person_id).toLong, x.observation_concept_id.toLong,  PatientObservationProperty(x))) 
    val labpatEdges: RDD[Edge[EdgeProperty]] = labResults.map(x => Edge(x.observation_concept_id.toLong, (-x.person_id).toLong, PatientObservationProperty(x))) 

    val patdiagEdges: RDD[Edge[EdgeProperty]] = diagnostics.map(x => Edge((-x.person_id).toLong, x.condition_concept_id.toLong,  PatientDiagnosticEdgeProperty(x)))
    val diagpatEdges: RDD[Edge[EdgeProperty]] = diagnostics.map(x => Edge(x.condition_concept_id.toLong, (-x.person_id).toLong, PatientDiagnosticEdgeProperty(x)))

    val medicationEdges:RDD[Edge[EdgeProperty]] = medications.map(e => Edge(-e.person_id.toLong, e.drug_concept_id.toLong, PatientMedicationEdgeProperty(e)))
    val revMedicationEdges:RDD[Edge[EdgeProperty]] = medications.map(e => Edge(e.drug_concept_id.toLong, -e.person_id.toLong, PatientMedicationEdgeProperty(e)))

    val vertices = snomedVertices.union(rxnormVertices).union(loincVertices).union(patientVertices)
    val edges = snomedEdges.union(rxnormEdges).union(patlabEdges).union(labpatEdges).union(patdiagEdges).union(diagpatEdges).union(medicationEdges).union(revMedicationEdges)
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)
    println("all vertices3: ", graph.vertices.count)
    println("all edges3: ", graph.edges.count)
    
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
