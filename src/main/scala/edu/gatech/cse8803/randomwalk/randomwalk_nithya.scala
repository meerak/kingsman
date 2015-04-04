package edu.gatech.cse8803.randomwalk
/*
 * @author rchen
 */
/**

students: please put your implementation in this file!
  **/

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: String , numIter: Int = 10, alpha: Double = 0.15): List[String] = {
    //compute ready state probabilities between patient patientID (NOT VERTEX ID) and all other patients and return the top 10 similar patients

    val sc = graph.vertices.sparkContext

    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      .mapVertices( (id, attr) => if(id == patientID.toLong) 1.0 else 0.0 )

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      prevRankGraph = rankGraph
      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => if(id == patientID.toLong) alpha + (1.0 - alpha) * msgSum else (1.0 - alpha) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) 
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }
    println("RandomWalk")
    
    val patient_vertices = graph.outerJoinVertices(rankGraph.vertices) {
      case(id, attr, Some(value)) => (attr, value)
      case(id, attr, None) => (attr, 0.0)
    }

    val similar_patients = patient_vertices.vertices.filter(a => a._2._1.isInstanceOf[PatientProperty] && a._1.toString != patientID)

    val sorted_vertices = similar_patients.top(10)(Ordering.by(_._2._2))
    val sorted_RDD = sorted_vertices.map(a => a._1.toString)

 
    sorted_RDD.toList
  }

  def summarize(graph: Graph[VertexProperty, EdgeProperty], patientIDs: List[String] ): (List[String], List[String], List[String])  = {

    val diagnostic_neighbors : VertexRDD[Int] = graph.aggregateMessages[Int](

      triplet => { 
          triplet.dstAttr match { case x : DiagnosticProperty => if (patientIDs contains triplet.srcId.toString) triplet.sendToDst(1)
                                  case _ => 
                                }
      },
      (a, b) => (a + b)
    )

    val medication_neighbors : VertexRDD[Int] = graph.aggregateMessages[Int](
      triplet => { 
          triplet.dstAttr match { case x : MedicationProperty => if (patientIDs contains triplet.srcId.toString) triplet.sendToDst(1)
                                  case _ =>
                               }
      },
    (a, b) => (a + b)
    )

    val labresults_neighbors : VertexRDD[Int] = graph.aggregateMessages[Int](
      triplet => { 
          triplet.dstAttr match { case x : LabResultProperty => if (patientIDs contains triplet.srcId.toString) triplet.sendToDst(1)
                                  case _ => 
          }
      },
    (a, b) => (a + b)
    )

    var vertices_keyed = graph.vertices.keyBy(_._1)

    var lab_keyed = labresults_neighbors.keyBy(_._1)
    val lab_joined = lab_keyed.join(vertices_keyed)
    val lab_RDD = lab_joined.map(a => (a._2._1._2, a._2._2._2.asInstanceOf[LabResultProperty].testName))
    val lab_sorted = lab_RDD.collect().sortWith(_._1 > _._1)
    val top_labs = lab_sorted.map(a => a._2.toString).take(5).toList

    var med_keyed = medication_neighbors.keyBy(_._1)
    val med_joined = med_keyed.join(vertices_keyed)
    val med_RDD = med_joined.map(a => (a._2._1._2, a._2._2._2.asInstanceOf[MedicationProperty].medicine))
    val med_sorted = med_RDD.collect().sortWith(_._1 > _._1)
    val top_medications = med_sorted.map(a => a._2.toString).take(5).toList

    var diag_keyed = diagnostic_neighbors.keyBy(_._1)
    val diag_joined = diag_keyed.join(vertices_keyed)
    val diag_RDD = diag_joined.map(a => (a._2._1._2, a._2._2._2.asInstanceOf[DiagnosticProperty].icd9code))
    val diag_sorted = diag_RDD.collect().sortWith(_._1 > _._1)
    val top_diagnoses = diag_sorted.map(a => a._2.toString).take(5).toList

    (top_diagnoses, top_medications, top_labs)
  }

}
