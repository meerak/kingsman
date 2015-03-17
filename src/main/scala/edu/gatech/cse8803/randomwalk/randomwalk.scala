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

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: String , numIter: Int = 10, alpha: Double = 0.15): List[String] = {
    //compute ready state probabilities between patient patientID (NOT VERTEX ID) and all other patients and return the top 10 similar patients
     // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices(
            (id, attr) => id match{
                case a if (a == patientID.toLong) => 1.0
                case _ => 0.0
            }
            //(id, attr) => alpha
        )

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => (1.0 - alpha) * msgSum + alpha*(if(id==patientID.toLong) 1.0 else 0.0)
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    val maxPatientVertexID = graph.vertices.filter { case (id, vertex) => vertex.isInstanceOf[PatientProperty]}.map(f=>f._2.asInstanceOf[PatientProperty].patientID).toArray.maxBy(f=>f.toLong).toLong

    val top10 = rankGraph.vertices.filter(x=> ((x._1 <= maxPatientVertexID)&& x._1!=patientID.toLong)).top(10) {
        Ordering.by((entry: (VertexId, Double)) => entry._2)
    }.map(x=>(x._1.toString)).toList

    //l.foreach(println)

    top10
  }

  def summarize(graph: Graph[VertexProperty, EdgeProperty], patientIDs: List[String] ): (List[String], List[String], List[String])  = {

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

    //val top_labs = List("test_name1" , "test_name2", "test_name3", "test_name4", "test_name5")
    (top_medications, top_diagnoses, top_labs)
  }

}
