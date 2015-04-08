package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._

object RandomWalk 
{
	def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: String , numIter: Int = 10, alpha: Double = 0.15): List[String] = 
	{
		val bcStartIndex = graph.vertices.sparkContext.broadcast(patientID.toLong)

	    var rankGraph: Graph[Double, Double] = graph
	      .outerJoinVertices(graph.outDegrees) 
	      { 
	      	(vid, vdata, deg) => deg.getOrElse(0) 
	      }
	      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
	      .mapVertices
	      { 
	      	(id, attr) => 
	      		if(id == bcStartIndex.value) 1.0 
	      		else 0.0
      	  }

	    var iteration = 0
	    var prevRankGraph: Graph[Double, Double] = null
	    while (iteration < numIter) 
	    {
	      rankGraph.cache()

	      val rankUpdates = rankGraph.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

	      prevRankGraph = rankGraph

	      rankGraph = rankGraph.joinVertices(rankUpdates) 
	      {
	        (id, oldRank, msgSum) => 
	        {
          		if(id == bcStartIndex.value) alpha +  (1.0 - alpha) * msgSum else (1.0 - alpha) * msgSum
          	}
	      }.cache()

	      rankGraph.edges.foreachPartition(x => {}) 
	      prevRankGraph.vertices.unpersist(false)
	      prevRankGraph.edges.unpersist(false)

	      iteration += 1
	    }

	    val richRankGraph =rankGraph.outerJoinVertices(graph.vertices) 
	    {
      		(id, rank, property) => (rank, property.orNull)
    	}
    	
    	richRankGraph.vertices
      	.filter(_._1 != bcStartIndex.value)
      	.filter(_._2._2 != null)
      	.filter(_._2._2.isInstanceOf[PatientProperty])
      	.takeOrdered(10)(scala.Ordering.by(-_._2._1))
      	.map((-_._1))
      	.collect{case id: Long => id}
      	.toList
      	.map(_.toString)

  	}
}
