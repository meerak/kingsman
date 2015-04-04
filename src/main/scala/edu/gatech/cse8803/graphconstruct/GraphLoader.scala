package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import edu.gatech.cse8803.enums._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object GraphLoader {
    private val LOG = LoggerFactory.getLogger(getClass())
  def load(patients: RDD[PatientProperty], medications: RDD[Medication], labResults:RDD[Observation], diagnostics: RDD[Diagnostic], rxnorm:RDD[Vocabulary], loinc: RDD[Vocabulary], snomed:RDD[Vocabulary], snomed_ancestors:RDD[ConceptAncestor], rxnorm_ancestors:RDD[ConceptAncestor], snomed_relations:RDD[ConceptRelation], rxnorm_relations:RDD[ConceptRelation], loinc_relations:RDD[ConceptRelation]): Graph[VertexProperty, EdgeProperty] = {

    val startTime = System.currentTimeMillis();
    LOG.info("Building graph")
        
    val patientVertices: RDD[(VertexId, VertexProperty)] = patients.map(p => ((-p.person_id).toLong, p))
    
    val loincVertices: RDD[(VertexId, VertexProperty)] = loinc.map(a=>(a.concept_id.toLong, ObservationProperty(a.concept_id)))
    val loincRelationEdges: RDD[Edge[EdgeProperty]] = loinc_relations.map(a=>Edge(a.source.toLong, a.dest.toLong, ConceptRelationEdgeProperty(a.relation)))

    val snomedVertices: RDD[(VertexId, VertexProperty)] = snomed.map(a=>(a.concept_id.toLong, DiagnosticProperty(a.concept_id)))
    val snomedEdges: RDD[Edge[EdgeProperty]] = snomed_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))
    val snomedDescendantEdges: RDD[Edge[EdgeProperty]] = snomed_ancestors.map(a=>Edge(a.ancestor_concept_id.toLong, a.descendent_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.CHILD)))
    val snomedRelationEdges: RDD[Edge[EdgeProperty]] = snomed_relations.map(a=>Edge(a.source.toLong, a.dest.toLong, ConceptRelationEdgeProperty(a.relation)))

    val rxnormVertices: RDD[(VertexId, VertexProperty)] = rxnorm.map(a=>(a.concept_id.toLong, MedicationProperty(a.concept_id)))
    val rxnormEdges: RDD[Edge[EdgeProperty]] = rxnorm_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))
    val rxnormDescendantEdges: RDD[Edge[EdgeProperty]] = rxnorm_ancestors.map(a=>Edge(a.ancestor_concept_id.toLong, a.descendent_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.CHILD)))
    val rxnormRelationEdges: RDD[Edge[EdgeProperty]] = rxnorm_relations.map(a=>Edge(a.source.toLong, a.dest.toLong, ConceptRelationEdgeProperty(a.relation)))

    val patlabEdges: RDD[Edge[EdgeProperty]] = labResults.map(x => Edge((-x.person_id).toLong, x.observation_concept_id.toLong,  PatientObservationProperty(x))) 
    val labpatEdges: RDD[Edge[EdgeProperty]] = labResults.map(x => Edge(x.observation_concept_id.toLong, (-x.person_id).toLong, PatientObservationProperty(x))) 

    val patdiagEdges: RDD[Edge[EdgeProperty]] = diagnostics.map(x => Edge((-x.person_id).toLong, x.condition_concept_id.toLong,  PatientDiagnosticEdgeProperty(x)))
    val diagpatEdges: RDD[Edge[EdgeProperty]] = diagnostics.map(x => Edge(x.condition_concept_id.toLong, (-x.person_id).toLong, PatientDiagnosticEdgeProperty(x)))

    val medicationEdges:RDD[Edge[EdgeProperty]] = medications.map(e => Edge(-e.person_id.toLong, e.drug_concept_id.toLong, PatientMedicationEdgeProperty(e)))
    val revMedicationEdges:RDD[Edge[EdgeProperty]] = medications.map(e => Edge(e.drug_concept_id.toLong, -e.person_id.toLong, PatientMedicationEdgeProperty(e)))

    val vertices = snomedVertices.union(rxnormVertices).union(loincVertices).union(patientVertices)
    val edges = snomedEdges.union(rxnormEdges).union(patlabEdges).union(labpatEdges).union(patdiagEdges).union(diagpatEdges).union(medicationEdges).union(revMedicationEdges).union(loincRelationEdges).union(rxnormRelationEdges).union(snomedRelationEdges).union(snomedDescendantEdges).union(rxnormDescendantEdges)
    //val edges = patlabEdges.union(labpatEdges).union(patdiagEdges).union(diagpatEdges).union(medicationEdges).union(revMedicationEdges)

    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)
    println("all vertices 1 : ", graph.vertices.count)
    println("all edges 1: ", graph.edges.count)
    
    val endTime = System.currentTimeMillis();
    LOG.info("Graph built in " + (endTime - startTime) + "ms")

    graph
  }
  
  def runPageRank(graph:  Graph[VertexProperty, EdgeProperty] ): List[(Long, Double)] ={
    //run pagerank provided by GraphX
    //return the top 5 mostly highly ranked vertices
    //for each vertex return the vertex name, which can be patientID, test_name or medication name and the corresponding rank
    //see example below
    
    val prGraph = graph.staticPageRank(10, 0.15).cache
    /*val prGraphProp = graph.outerJoinVertices(prGraph.vertices) {
        (id, rank) => (VertexProperty match{ 
        case a: PatientProperty => a.person_id
        case b: MedicationProperty => b.drug_concept_id.toLong
        case c: ObservationProperty => c.observation_concept_id.toLong
        case d: DiagnosticProperty => d.condition_concept_id.toLong
        case e: VocabularyProperty => e.concept_id.toLong
        },  rank.getOrElse(0.0))
    }*/
    val  top = prGraph.vertices.top(5) {
        Ordering.by((entry: (VertexId, Double)) => entry._2)
    }

    val p  = top.map(t=> (t._1 , t._2)).toList
    p
  }
}
