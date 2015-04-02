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

      //rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    //val maxPatientVertexID = graph.vertices.filter { case (id, vertex) => vertex.isInstanceOf[PatientProperty]}.map(f=>f._2.asInstanceOf[PatientProperty].patientID).toArray.maxBy(f=>f.toLong).toLong
    val top10 = rankGraph.vertices.filter(x=> ((x._1 <= 0)&& x._1!=patientID.toLong)).top(10) 
    {
        Ordering.by((entry: (VertexId, Double)) => entry._2)
    }.toList
    //.map(x=>(x._1.toString)).toList



    top10.foreach(println)
    null
  }
}