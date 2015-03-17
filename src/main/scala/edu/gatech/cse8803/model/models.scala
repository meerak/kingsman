package edu.gatech.cse8803.model

case class Observation(person_id:Int, observation_concept_id: Int, observation_date: String, observation_time: String, vale_as_number: Float, 
    value_as_string:String, value_as_concept_id:Int, unit_concept_id: Int, range_low: Float, range_high: Float, observation_type_concept_id: Int, associated_provider_id: Int
    visit_occurrence_id: Int, relevant_condition_concept_id:Int, observation_source_value:String, units_source_value: String)

case class Diagnostic(condition_occurrence_id: Integer, person_id: Integer, condition_concept_id: Integer, condition_start_date: String, condition_end_date: String, condition_type_concept_id: Integer, stop_reason: String, associated_provider_id: Integer, visit_occurrence_id: Integer, condition_source_value: String)

case class Medication(drug_exposure_id: Integer, person_id: Integer, drug_concept_id: Integer, drug_exposure_start_date: String, drug_exposure_end_date: String, drug_type_concept_id: Integer, stop_reason: String, refills: Integer, quantity: Integer, days_supply: Integer, sig: String, prescribing_provider_id: Integer, visit_occurrence_id: Integer, relevant_condition_concept_id: Integer, drug_source_value: String)

abstract class VertexProperty

case class PatientProperty(person_id: Integer, gender_concept_id: Integer, year_of_birth: Integer, month_of_birth: Integer, day_of_birth: Integer, race_concept_id: Integer, ethnicity_concept_id: Integer, location_id: Integer, provider_id: Integer, care_site_id: Integer, person_source_value: String, gender_source_value: String, race_source_value: String, ethnicity_source_value: String) extends VertexProperty

case class ObservationProperty(observation_concept_id: Int) extends VertexProperty

case class DiagnosticProperty(condition_concept_id: Integer) extends VertexProperty

case class MedicationProperty(drug_concept_id: Integer) extends VertexProperty

abstract class EdgeProperty

case class SampleEdgeProperty(name: String = "Sample") extends EdgeProperty

case class PatientObservationProperty(observation_date: String, observation_time: String, vale_as_number: Float, 
    value_as_string:String, value_as_concept_id:Int, unit_concept_id: Int, range_low: Float, range_high: Float, observation_type_concept_id: Int, associated_provider_id: Int
    visit_occurrence_id: Int, relevant_condition_concept_id:Int, observation_source_value:String, units_source_value: String) extends EdgeProperty

case class PatientDiagnosticEdgeProperty(diagnostic: Diagnostic) extends EdgeProperty

case class PatientMedicationEdgeProperty(medication: Medication) extends EdgeProperty

case class conceptAncestorEdgeProperty(relation: String = "IS-A") extends EdgeProperty

