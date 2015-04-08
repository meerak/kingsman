package edu.gatech.cse8803.metrics

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Jaccard {
    
   def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: String): List[String] = 
   {
        val bcSrcVertexId = graph.edges.sparkContext.broadcast(patientID.toLong)

        val srcCharacteristic = graph.edges.filter{case Edge(srcId, dstId, edgeProperty) => srcId == bcSrcVertexId.value}.map{_.dstId}.collect().toSet
        val bcSrcCharacteristic = graph.edges.sparkContext.broadcast(srcCharacteristic)

        graph
          .collectNeighbors(EdgeDirection.Out)
          .filter{case (idx, neighbors) => neighbors.size > 0 && !neighbors.exists{case(_, p) => p.isInstanceOf[PatientProperty]}}
          .mapValues
          {
            neighbors:Array[(VertexId, VertexProperty)] =>
            val characteristic = neighbors.map(_._1).toSet
            val intersect = bcSrcCharacteristic.value.intersect(characteristic)
            val union = bcSrcCharacteristic.value.union(characteristic)
            if(union.size ==0) 0.0 
            else intersect.size.toDouble / union.size.toDouble
          }
          .filter(_._2 > 0.0)
          .filter(x => x._1 != bcSrcVertexId.value)
          .takeOrdered(10)(scala.Ordering.by(-_._2))
          .map(_._1.toString())
          .toList
    }
}
