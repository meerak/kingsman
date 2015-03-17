 /**

  students: please put your implementation in this file!
**/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: String, wd: Double, wm: Double, wl: Double): List[String] = {
    //compute ready state probabilities between patient patientID (NOT VERTEX ID) and all other patients and return the top 10 similar patients

    val patientEdges = graph.triplets.filter(t=>(t.srcAttr.isInstanceOf[PatientProperty] && t.srcId!= patientID.toLong)).cache()
    val labEdges =  patientEdges.filter(t=>t.dstAttr.isInstanceOf[LabResultProperty]).map(x=> (x.srcId, x.dstId)).groupBy(_._1).map(m=>(m._1, m._2.toList.map(y=>y._2)))
    val medicationEdges = patientEdges.filter(t=>t.dstAttr.isInstanceOf[MedicationProperty]).map(x=> (x.srcId, x.dstId)).groupBy(_._1).map(m=>(m._1, m._2.toList.map(y=>y._2)))
    val diagnosticsEdges = patientEdges.filter(t=>t.dstAttr.isInstanceOf[DiagnosticProperty]).map(x=> (x.srcId, x.dstId)).groupBy(_._1).map(m=>(m._1, m._2.toList.map(y=>y._2)))
    
    val compPatient = graph.triplets.filter(t=>(t.srcId== patientID.toLong && t.srcAttr.isInstanceOf[PatientProperty]))
    val compLab = compPatient.filter(t=>t.dstAttr.isInstanceOf[LabResultProperty]).map(x=> (x.srcId, x.dstId)).groupBy(_._1).map(m=>m._2.toList.map(y=>y._2)).collect
    val compMedication = compPatient.filter(t=>t.dstAttr.isInstanceOf[MedicationProperty]).map(x=> (x.srcId, x.dstId)).groupBy(_._1).map(m=>m._2.toList.map(y=>y._2)).collect
    val compDiagnostics = compPatient.filter(t=>t.dstAttr.isInstanceOf[DiagnosticProperty]).map(x=> (x.srcId, x.dstId)).groupBy(_._1).map(m=>m._2.toList.map(y=>y._2)).collect
    
    val jaccardLab:RDD[(VertexId, Double)] = labEdges.map(x=>
        if(compLab.size!=0 && x._2.union(compLab(0)).size!=0) 
            (x._1, wl*x._2.intersect(compLab(0)).size/x._2.union(compLab(0)).distinct.size.toFloat)
        else (x._1, 0F)
    )
    
    val jaccardMedication:RDD[(VertexId, Double)] = medicationEdges.map(x=>
        if(compMedication.size!=0 && x._2.union(compMedication(0)).size!=0) 
                (x._1, wm*x._2.intersect(compMedication(0)).size/x._2.union(compMedication(0)).distinct.size.toFloat)
        else (x._1, 0F)
    )
    
    val jaccardDiagnostics:RDD[(VertexId, Double)] = diagnosticsEdges.map(x=>
        if(compDiagnostics.size!=0 && x._2.union(compDiagnostics(0)).size!=0) 
            (x._1, wd*x._2.intersect(compDiagnostics(0)).size/x._2.union(compDiagnostics(0)).distinct.size.toFloat)
        else (x._1, 0F)
    )
    
    val jaccard = jaccardLab.union(jaccardMedication).union(jaccardDiagnostics).reduceByKey(_+_)
    
    val top_jaccard = jaccard.top(10) {
        Ordering.by((entry: (VertexId, Double)) => entry._2)
    }
    
    top_jaccard.map(x=>x._1.toString).toList
  }

  def summarize(graph: Graph[VertexProperty, EdgeProperty] , patientIDs: List[String]): (List[String], List[String], List[String])  = {

    val patientRelatedEdges = graph.triplets.filter(t=>(patientIDs.contains(t.srcId.toString))).cache()
    
    //patientRelatedEdges.foreach(println)
    val lab  = patientRelatedEdges.filter(x=>x.dstAttr.isInstanceOf[LabResultProperty]).map(x=>(x.dstAttr.asInstanceOf[LabResultProperty].testName.toString->1)).reduceByKey(_+_)
    val medication  = patientRelatedEdges.filter(x=>x.dstAttr.isInstanceOf[MedicationProperty]).map(x=>(x.dstAttr.asInstanceOf[MedicationProperty].medicine->1)).reduceByKey(_+_)
    val diagnostics  = patientRelatedEdges.filter(x=>x.dstAttr.isInstanceOf[DiagnosticProperty]).map(x=>(x.dstAttr.asInstanceOf[DiagnosticProperty].icd9code->1)).reduceByKey(_+_)
    
    //replace top_diagnosis with the most frequently used diagnosis in the top most similar patients
    //must contains ICD9 codes
    //val top_diagnoses = List("ICD9-1" , "ICD9-2", "ICD9-3", "ICD9-4", "ICD9-5")
    val top_diagnoses = diagnostics.top(5) {
        Ordering.by((entry: (String, Int)) => entry._2)
    }.map(x=>x._1.toString).toList

    //replace top_medications with the most frequently used medications in the top most similar patients
    //must contain medication name
    //val top_medications = List("med_name1" , "med_name2", "med_name3", "med_name4", "med_name5")
    val top_medications = medication.top(5) {
        Ordering.by((entry: (String, Int)) => entry._2)
    }.map(x=>x._1).toList
    
    //replace top_labs with the most frequently used labs in the top most similar patients
    //must contain test name
    val top_labs = lab.top(5) {
        Ordering.by((entry: (String, Int)) => entry._2)
    }.map(x=>x._1).toList

    /*println(patientRelatedEdges.count)
    val temp = patientRelatedEdges.map(x=> x.dstAttr match{
        case a: LabResultProperty => "L"->x.dstId
        case b: MedicationProperty => "M" -> x.dstId
        case c: DiagnosticProperty => "D"->x.dstId
        }).groupByKey()
    val lab = temp.lookup("L")(0).toList.map(x=>(x->1)).groupBy(_._1).mapValues(_.size)*/

    (top_medications, top_diagnoses, top_labs)
  }

}