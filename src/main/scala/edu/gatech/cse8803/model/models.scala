/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

case class Observation(personID:Int, observationConceptID: Int, observationDate: String, observationTime: String, valeAsNumber: Float, 
    valueAsString:String, valueAsConceptId:Int, unitConceptId: Int, rangeLow: Float, rangeHigh: Float, observationTypeConceptId: Int, associatedProviderId: Int
    visitOccurrenceId: Int, relevantConditionConceptId:Int, observationSourceValue:String, unitsSourceValue: String)

case class Diagnostic(patientID: String, date: Long, icd9code: String, sequence: Int)

case class Medication(patientID: String, date: Long, medicine: String)

abstract class VertexProperty

case class PatientProperty(patientID: String, sex: String, dob: String, dod: String) extends VertexProperty

case class ObservationProperty(observationConceptID: Int) extends VertexProperty

case class DiagnosticProperty(icd9code: String) extends VertexProperty

case class MedicationProperty(medicine: String) extends VertexProperty

abstract class EdgeProperty

case class SampleEdgeProperty(name: String = "Sample") extends EdgeProperty

case class PatientObservationProperty(observationDate: String, observationTime: String, valeAsNumber: Float, 
    valueAsString:String, valueAsConceptId:Int, unitConceptId: Int, rangeLow: Float, rangeHigh: Float, observationTypeConceptId: Int, associatedProviderId: Int
    visitOccurrenceId: Int, relevantConditionConceptId:Int, observationSourceValue:String, unitsSourceValue: String) extends EdgeProperty

case class PatientDiagnosticEdgeProperty(diagnostic: Diagnostic) extends EdgeProperty

case class PatientMedicationEdgeProperty(medication: Medication) extends EdgeProperty

