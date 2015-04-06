package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import java.util.Calendar
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import edu.gatech.cse8803.enums._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object GraphLoader 
{
    private val LOG = LoggerFactory.getLogger(getClass())
    def load(patients: RDD[PatientProperty], medications: RDD[Medication], labResults:RDD[Observation], diagnostics: RDD[Diagnostic], age: RDD[AgeProperty], gender: RDD[Vocabulary], race: RDD[Vocabulary], rxnorm:RDD[Vocabulary], loinc: RDD[Vocabulary], snomed:RDD[Vocabulary], race_ancestors:RDD[ConceptAncestor], snomed_ancestors:RDD[ConceptAncestor], rxnorm_ancestors:RDD[ConceptAncestor], race_relations:RDD[ConceptRelation], snomed_relations:RDD[ConceptRelation], rxnorm_relations:RDD[ConceptRelation], loinc_relations:RDD[ConceptRelation]): Graph[VertexProperty, EdgeProperty] = 
    {
    val startTime = System.currentTimeMillis();
    LOG.info("Building graph")
        
    val patientVertices: RDD[(VertexId, VertexProperty)] = patients.map(p => ((-p.person_id).toLong, p))
    //println("Patients")

    val ageVertices: RDD[(VertexId, VertexProperty)] = age.map(a => (-a.age_range.toLong, a))
    //println("Age")

    val genderVertices: RDD[(VertexId, VertexProperty)] = gender.map(a=>(a.concept_id.toLong, GenderProperty(a.concept_id)))
    //println("Gender")

    val raceVertices: RDD[(VertexId, VertexProperty)] = race.map(a=>(a.concept_id.toLong, RaceProperty(a.concept_id)))
    //println("Race")
    val raceEdges: RDD[Edge[EdgeProperty]] = race_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))
    val raceDescendantEdges: RDD[Edge[EdgeProperty]] = race_ancestors.map(a=>Edge(a.ancestor_concept_id.toLong, a.descendent_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.CHILD)))
    //println("Race Ancestors")
    val raceRelationEdges: RDD[Edge[EdgeProperty]] = race_relations.map(a=>Edge(a.source.toLong, a.dest.toLong, ConceptRelationEdgeProperty(a.relation)))

    val patraceEdges:RDD[Edge[EdgeProperty]] = patients.map(e => Edge(-e.person_id.toLong, e.race_concept_id.toLong, PatientRaceEdgeProperty(Race(-e.person_id.toLong, e.race_concept_id))))
    val racepatEdges:RDD[Edge[EdgeProperty]] = patients.map(e => Edge(e.race_concept_id.toLong, -e.person_id.toLong, PatientRaceEdgeProperty(Race(-e.person_id.toLong, e.race_concept_id))))
    //println("Race Edges")

    val patgenEdges: RDD[Edge[EdgeProperty]] = patients.map(e => Edge(-e.person_id.toLong, e.gender_concept_id.toLong, PatientGenderEdgeProperty(Gender(-e.person_id.toLong, e.gender_concept_id))))
    val genpatEdges:RDD[Edge[EdgeProperty]] = patients.map(e => Edge(e.gender_concept_id.toLong, -e.person_id.toLong, PatientGenderEdgeProperty(Gender(-e.person_id.toLong, e.gender_concept_id))))
    //println("Gender Edges")

    val year=Calendar.getInstance().get(Calendar.YEAR);
    
    val patageEdges: RDD[Edge[EdgeProperty]] = patients.map(e => Edge(-e.person_id.toLong, -10*((year - e.year_of_birth) / 10), PatientAgeEdgeProperty(Age(-e.person_id.toLong, -10*((year - e.year_of_birth) / 10)))))
    val agepatEdges: RDD[Edge[EdgeProperty]] = patients.map(e => Edge(-10*((year - e.year_of_birth) / 10), -e.person_id.toLong, PatientAgeEdgeProperty(Age(-e.person_id.toLong, -10*((year - e.year_of_birth) / 10)))))
    //println("Age Edges")

    val loincVertices: RDD[(VertexId, VertexProperty)] = loinc.map(a=>(a.concept_id.toLong, ObservationProperty(a.concept_id)))
    //println("Loinc")
    val loincRelationEdges: RDD[Edge[EdgeProperty]] = loinc_relations.map(a=>Edge(a.source.toLong, a.dest.toLong, ConceptRelationEdgeProperty(a.relation)))

    val snomedVertices: RDD[(VertexId, VertexProperty)] = snomed.map(a=>(a.concept_id.toLong, DiagnosticProperty(a.concept_id)))
    //println("Snomed")
    val snomedEdges: RDD[Edge[EdgeProperty]] = snomed_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))
    val snomedDescendantEdges: RDD[Edge[EdgeProperty]] = snomed_ancestors.map(a=>Edge(a.ancestor_concept_id.toLong, a.descendent_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.CHILD)))
    //println("Snomed Ancestors")
    val snomedRelationEdges: RDD[Edge[EdgeProperty]] = snomed_relations.map(a=>Edge(a.source.toLong, a.dest.toLong, ConceptRelationEdgeProperty(a.relation)))

    val rxnormVertices: RDD[(VertexId, VertexProperty)] = rxnorm.map(a=>(a.concept_id.toLong, MedicationProperty(a.concept_id)))
    //println("RxNorm")
    val rxnormEdges: RDD[Edge[EdgeProperty]] = rxnorm_ancestors.map(a=>Edge(a.descendent_concept_id.toLong, a.ancestor_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.ISA)))
    val rxnormDescendantEdges: RDD[Edge[EdgeProperty]] = rxnorm_ancestors.map(a=>Edge(a.ancestor_concept_id.toLong, a.descendent_concept_id.toLong, ConceptAncestorEdgeProperty(Enumerations.CHILD)))
    //println("RxNorm Ancestors")
    val rxnormRelationEdges: RDD[Edge[EdgeProperty]] = rxnorm_relations.map(a=>Edge(a.source.toLong, a.dest.toLong, ConceptRelationEdgeProperty(a.relation)))

    val patlabEdges: RDD[Edge[EdgeProperty]] = labResults.map(x => Edge((-x.person_id).toLong, x.observation_concept_id.toLong,  PatientObservationProperty(x))) 
    val labpatEdges: RDD[Edge[EdgeProperty]] = labResults.map(x => Edge(x.observation_concept_id.toLong, (-x.person_id).toLong, PatientObservationProperty(x))) 
    //println("Lab Edges")

    val patdiagEdges: RDD[Edge[EdgeProperty]] = diagnostics.map(x => Edge((-x.person_id).toLong, x.condition_concept_id.toLong,  PatientDiagnosticEdgeProperty(x)))
    val diagpatEdges: RDD[Edge[EdgeProperty]] = diagnostics.map(x => Edge(x.condition_concept_id.toLong, (-x.person_id).toLong, PatientDiagnosticEdgeProperty(x)))
    //println("Diagnosis Edges")

    val medicationEdges:RDD[Edge[EdgeProperty]] = medications.map(e => Edge(-e.person_id.toLong, e.drug_concept_id.toLong, PatientMedicationEdgeProperty(e)))
    val revMedicationEdges:RDD[Edge[EdgeProperty]] = medications.map(e => Edge(e.drug_concept_id.toLong, -e.person_id.toLong, PatientMedicationEdgeProperty(e)))
    //println("Medication Edges")
    
    val vertices = raceVertices.union(patientVertices).union(genderVertices).union(ageVertices).union(loincVertices).union(rxnormVertices).union(snomedVertices)
    //println("Vertices Done", vertices.count)
    val edges1 = raceEdges.union(racepatEdges).union(patraceEdges).union(raceDescendantEdges).union(genpatEdges).union(patgenEdges).union(patageEdges).union(agepatEdges)
    //println("Edges 1 Done")
    val edges = edges1.union(rxnormRelationEdges).union(loincRelationEdges).union(snomedRelationEdges).union(raceRelationEdges).union(patdiagEdges).union(diagpatEdges).union(snomedEdges).union(snomedDescendantEdges).union(patlabEdges).union(labpatEdges).union(medicationEdges).union(rxnormEdges).union(rxnormDescendantEdges).union(revMedicationEdges)
    //println("Edges Done", edges.count)

    val graph: Graph[VertexProperty, EdgeProperty] = Graph(vertices, edges)
    println("all vertices 1 : ", graph.vertices.count)
    println("all edges 1: ", graph.edges.count)
    
    val endTime = System.currentTimeMillis();
    LOG.info("Graph built in " + (endTime - startTime) + "ms")

    graph
  }
  
  def runPageRank(graph:  Graph[VertexProperty, EdgeProperty] ): List[(Long, Double)] =
  {
    
    val prGraph = graph.staticPageRank(10, 0.15).cache
    
    val  top = prGraph.vertices.top(5) {
        Ordering.by((entry: (VertexId, Double)) => entry._2)
    }

    val p  = top.map(t=> (t._1 , t._2)).toList
    p
  }
}
