package edu.gatech.cse8803.metrics

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import scala.collection.mutable.Map
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object CosineSimilarity {

  def cosineSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: String, wd: Double, wm: Double, wl: Double): List[String] = {
    //compute ready state probabilities between patient patientID (NOT VERTEX ID) and all other patients and return the top 10 similar patients

    val query_patient = graph.vertices.filter{case(id, _) => id == patientID.toLong}

    val patient_neighbors_RDD : VertexRDD[Int] = collectNeighbors(graph, patientID)

    val patient_neighbors_arr = patient_neighbors_RDD.collect()
    var patient_neighbors_map = Map[Long, Int]()
    for(i <- patient_neighbors_arr) {
      patient_neighbors_map.put(i._1, 1 )
    }

    val diagnostic_neighbors_count: VertexRDD[(Int, Int)] = graph.aggregateMessages[(Int, Int)](
    triplet => { // Map Function
        // triplet.attr match {
            
        // }
          triplet.srcAttr match { case a : DiagnosticProperty => if (patient_neighbors_map contains triplet.srcId) triplet.sendToDst(1, 1) else triplet.sendToDst(0, 1)  
                                  case a : MedicationProperty => triplet.sendToDst(0, 0)
                                  case a : ObservationProperty => triplet.sendToDst(0, 0)
                                  case a : PatientProperty => triplet.sendToDst(0, 0)
                                  case a : VocabularyProperty => triplet.sendToDst(0, 0)
                                }
              },
          (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )

    val diag_count_filtered = diagnostic_neighbors_count.filter(a => a._1.toLong == patientID.toLong)
    val diag_all_count = diag_count_filtered.collect()(0)._2._2
    val diagnostic_similarity = diagnostic_neighbors_count.map( a => (a._1, a._2._1.toDouble/(Math.sqrt(a._2._2.toDouble * diag_all_count.toDouble)  + (1E-15))))
    val weighted_diagnostic_similarity = diagnostic_similarity.map(a => (a._1, a._2 * wd))

    val medication_neighbors_count: VertexRDD[(Int, Int)] = graph.aggregateMessages[(Int, Int)](
    triplet => { // Map Function
          triplet.srcAttr match { case a : MedicationProperty => if (patient_neighbors_map contains triplet.srcId) triplet.sendToDst(1, 1) else triplet.sendToDst(0, 1)  
                                  case a : DiagnosticProperty => triplet.sendToDst(0, 0)
                                  case a : ObservationProperty => triplet.sendToDst(0, 0)
                                  case a : PatientProperty => triplet.sendToDst(0, 0)
                                  case a : VocabularyProperty => triplet.sendToDst(0, 0)
                                }
              },
          (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )

    val med_count_filtered = medication_neighbors_count.filter(a => a._1.toLong == patientID.toLong)
    val med_all_count = med_count_filtered.collect()(0)._2._2
    val medication_similarity = medication_neighbors_count.map( a => (a._1, a._2._1.toDouble/(Math.sqrt(a._2._2.toDouble * med_all_count.toDouble) + (2E-15))))
    val weighted_medication_similarity = medication_similarity.map(a => (a._1, a._2 * wm))

    val labresults_neighbors_count: VertexRDD[(Int, Int)] = graph.aggregateMessages[(Int, Int)](
    triplet => { // Map Function
          triplet.srcAttr match { case a : ObservationProperty => if (patient_neighbors_map contains triplet.srcId) triplet.sendToDst(1, 1) else triplet.sendToDst(0, 1)  
                                  case a : DiagnosticProperty => triplet.sendToDst(0, 0)
                                  case a : MedicationProperty => triplet.sendToDst(0, 0)
                                  case a : PatientProperty => triplet.sendToDst(0, 0)
                                  case a : VocabularyProperty => triplet.sendToDst(0, 0)
                                }
              },
          (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )

    val lab_count_filtered = labresults_neighbors_count.filter(a => a._1.toLong == patientID.toLong)
    val lab_all_count = lab_count_filtered.collect()(0)._2._2
    val labresults_similarity = labresults_neighbors_count.map( a => (a._1, a._2._1.toDouble/(Math.sqrt(a._2._2.toDouble * lab_all_count.toDouble) + (2E-15))))
    val weighted_lab_similarity= labresults_similarity.map(a => (a._1, a._2 * wl))

    val diag_keyed = weighted_diagnostic_similarity.keyBy(_._1)
    val lab_keyed = weighted_lab_similarity.keyBy(_._1)
    val med_keyed = medication_similarity.keyBy(_._1)

    val joined_similarity = lab_keyed.join(diag_keyed).join(med_keyed)
    val final_similarity = joined_similarity.map(a => (a._1, a._2._1._1._2 + a._2._1._2._2 + a._2._2._2)).filter(_._1 != patientID.toLong)
    val similarity_arr = final_similarity.collect()
    val similarity_arr_sorted = similarity_arr.sortWith(_._2 > _._2)
    var top_10_patients = similarity_arr_sorted.take(50)

    var similar_patients_list = List[String]()
    for(i <- top_10_patients) {
      similar_patients_list = similar_patients_list :+ i._1.toString
      //println("Patient: " + i._1 +"Cosine: " + i._2)
    }

    similar_patients_list
  }

  def collectNeighbors(graph: Graph[VertexProperty, EdgeProperty], patientID: String): VertexRDD[Int] = {
    val neighbors_RDD : VertexRDD[Int] = graph.aggregateMessages[Int] (
        triplet => { if(triplet.srcId == patientID.toLong)
                      triplet.sendToDst(1)
          },
          (a,b) => (a+b)
    )
    neighbors_RDD
  }

  // def summarize(graph: Graph[VertexProperty, EdgeProperty] , patientIDs: List[String]): (List[String], List[String], List[String])  = {

  //   //replace top_diagnosis with the most frequently used diagnosis in the top most similar patients
  //   //must contains ICD9 codes
  //   val diagnostic_neighbors : VertexRDD[Int] = graph.aggregateMessages[Int](

  //     triplet => { 
  //         triplet.dstAttr match { case x : DiagnosticProperty => if (patientIDs contains triplet.srcId.toString) triplet.sendToDst(1)
  //                                 case _ => 
  //                               }
  //     },
  //     (a, b) => (a + b)
  //   )

  //   val medication_neighbors : VertexRDD[Int] = graph.aggregateMessages[Int](
  //     triplet => { 
  //         triplet.dstAttr match { case x : MedicationProperty => if (patientIDs contains triplet.srcId.toString) triplet.sendToDst(1)
  //                                 case _ =>
  //                              }
  //     },
  //   (a, b) => (a + b)
  //   )

  //   val labresults_neighbors : VertexRDD[Int] = graph.aggregateMessages[Int](
  //     triplet => { 
  //         triplet.dstAttr match { case x : LabResultProperty => if (patientIDs contains triplet.srcId.toString) triplet.sendToDst(1)
  //                                 case _ => 
  //         }
  //     },
  //   (a, b) => (a + b)
  //   )

  //   var vertices_keyed = graph.vertices.keyBy(_._1)

  //   var lab_keyed = labresults_neighbors.keyBy(_._1)
  //   val lab_joined = lab_keyed.join(vertices_keyed)
  //   val lab_RDD = lab_joined.map(a => (a._2._1._2, a._2._2._2.asInstanceOf[LabResultProperty].testName))
  //   val lab_sorted = lab_RDD.collect().sortWith(_._1 > _._1)
  //   val top_labs = lab_sorted.map(a => a._2.toString).take(5).toList

  //   var med_keyed = medication_neighbors.keyBy(_._1)
  //   val med_joined = med_keyed.join(vertices_keyed)
  //   val med_RDD = med_joined.map(a => (a._2._1._2, a._2._2._2.asInstanceOf[MedicationProperty].medicine))
  //   val med_sorted = med_RDD.collect().sortWith(_._1 > _._1)
  //   val top_medications = med_sorted.map(a => a._2.toString).take(5).toList

  //   var diag_keyed = diagnostic_neighbors.keyBy(_._1)
  //   val diag_joined = diag_keyed.join(vertices_keyed)
  //   val diag_RDD = diag_joined.map(a => (a._2._1._2, a._2._2._2.asInstanceOf[DiagnosticProperty].icd9code))
  //   val diag_sorted = diag_RDD.collect().sortWith(_._1 > _._1)
  //   val top_diagnoses = diag_sorted.map(a => a._2.toString).take(5).toList

  //   (top_diagnoses, top_medications, top_labs)

  // }

}