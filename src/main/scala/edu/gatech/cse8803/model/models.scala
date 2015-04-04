package edu.gatech.cse8803.model

import edu.gatech.cse8803.enums._

case class Observation(observation_id: Integer, person_id:Long ,observation_concept_id: Integer, observation_date: String, 
    observation_time: String, value_as_number: Float, value_as_string:String, value_as_concept_id:Integer,
    unit_concept_id: Integer, range_low: Float, range_high: Float, observation_type_concept_id: Integer,
    associated_provider_id: Int, visit_occurrence_id: BigDecimal, relevant_condition_concept_id:Integer, 
    observation_source_value:String, units_source_value: String)

case class Diagnostic(condition_occurrence_id: Integer, person_id: Long, condition_concept_id: Integer, condition_start_date: String, condition_end_date: String, condition_type_concept_id: Integer, stop_reason: String, associated_provider_id: Integer, visit_occurrence_id: BigDecimal, condition_source_value: String)

case class Medication(drug_exposure_id: Integer, person_id: Long, drug_concept_id: Integer, drug_exposure_start_date: String, drug_exposure_end_date: String, drug_type_concept_id: Integer, stop_reason: String, refills: Integer, quantity: Integer, days_supply: Integer, sig: String, prescribing_provider_id: Integer, visit_occurrence_id: BigDecimal, relevant_condition_concept_id: Integer, drug_source_value: String)

case class ConceptAncestor(ancestor_concept_id:Integer, descendent_concept_id:Integer)

case class ConceptRelation(source:Integer, dest:Integer, relation:String)

case class Vocabulary(concept_id:Integer, conecpt_name:String, concept_code:String)

abstract class VertexProperty

case class PatientProperty(person_id: Long, gender_concept_id: Integer, year_of_birth: Integer, month_of_birth: Integer, day_of_birth: Integer, race_concept_id: Integer, ethnicity_concept_id: Integer, location_id: Integer, provider_id: Integer, care_site_id: Integer, person_source_value: String, gender_source_value: String, race_source_value: String, ethnicity_source_value: String, dead:Integer) extends VertexProperty

case class ObservationProperty(observation_concept_id: Integer) extends VertexProperty

case class DiagnosticProperty(condition_concept_id: Integer) extends VertexProperty

case class MedicationProperty(drug_concept_id: Integer) extends VertexProperty

case class VocabularyProperty(concept_id: Integer) extends VertexProperty

abstract class EdgeProperty

case class SampleEdgeProperty(name: String = "Sample") extends EdgeProperty

case class PatientObservationProperty(observation: Observation) extends EdgeProperty

case class PatientDiagnosticEdgeProperty(diagnostic: Diagnostic) extends EdgeProperty

case class PatientMedicationEdgeProperty(medication: Medication) extends EdgeProperty

case class ConceptAncestorEdgeProperty(relation: Enumerations.Relation) extends EdgeProperty

case class ConceptRelationEdgeProperty(relation: String) extends EdgeProperty
