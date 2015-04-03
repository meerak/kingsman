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
import edu.gatech.cse8803.metrics._

object KNN 
{  
  def knnAllVsAll(graph:Graph[VertexProperty, EdgeProperty], patientIDtoLookup:String): Double = 
  {
    val top10 = CosineSimilarity.cosineSimilarityOneVsAll(graph, patientIDtoLookup, 1, 0, 0)
    val probabilityCount = graph.vertices.filter(x => top10.contains(x._1.toString)).map(x => (x._2.asInstanceOf[PatientProperty].dead, 1)).reduceByKey(_ + _)
    var one = probabilityCount.lookup(1)
    var zero = probabilityCount.lookup(0)
    if (one ==null)
        one = Seq(0)
    if (zero ==null)
        zero = Seq(0)
    val probability = one(0).toFloat / (zero(0).toFloat + one(0).toFloat)

    probability
  }
}
