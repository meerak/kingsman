package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import edu.gatech.cse8803.metrics._

object KNN 
{  
  def knnOneVsAll(graph:Graph[VertexProperty, EdgeProperty], patientIDtoLookup:String, casecontrol: List[String], metric: String): (List[String], Double) = 
  {
    var top10 : List[String] = List() 

    if(metric == "Cosine")
    {
      top10 = CosineSimilarity.cosineSimilarityOneVsAll(graph, patientIDtoLookup, casecontrol, 10)
    }
    else if(metric == "Jaccard")
    {
      top10 = Jaccard.jaccardSimilarityOneVsAll(graph, patientIDtoLookup, casecontrol)
    }
    else if(metric == "Minimum")
    {
      top10 = MinSimilarity.MinSimilarityOneVsAll(graph, patientIDtoLookup, casecontrol)
    }
    else
    {
      top10 = RandomWalk.randomWalkOneVsAll(graph, patientIDtoLookup, casecontrol)
    }

    val probabilityCount = graph.vertices.filter(x => top10.contains((-1 * x._1).toString)).map(x => (x._2.asInstanceOf[PatientProperty].dead, 1)).reduceByKey(_ + _)

    var one = probabilityCount.lookup(1)

    var zero = probabilityCount.lookup(0)

    if (one.size==0)
        one = Seq(0)
    if (zero.size==0)
        zero = Seq(0)
    val probability = one(0).toFloat / (zero(0).toFloat + one(0).toFloat)

    (top10, probability)
  }

  def summarize(graph: Graph[VertexProperty, EdgeProperty] , patientIDs: List[String]): (List[(String, Int)], List[(String, Int)], List[(String, Int)], List[(String, Int)], List[(String, Int)], List[(Int, Int)])  = 
  {
      val similar = graph.triplets.filter(x => patientIDs.contains((-1 * x.srcId).toString))

      val top_diagnoses = similar.filter(x => x.dstAttr.isInstanceOf[DiagnosticProperty])
      .map(x => (x.srcId, x.dstAttr.asInstanceOf[DiagnosticProperty].condition_concept_name))
      .distinct  
      .map(x => (x._2, (1))).reduceByKey(_ + _)
      .top(5) 
      {
          Ordering.by((entry: (String, (Int))) => entry._2)
      }.map(x => (x._1, x._2)).toList

      val top_medications = similar.filter(x => x.dstAttr.isInstanceOf[MedicationProperty])
      .map(x => (x.srcId, x.dstAttr.asInstanceOf[MedicationProperty].drug_concept_name))
      .distinct  
      .map(x => (x._2, (1))).reduceByKey(_ + _)
      .top(5) 
      {
          Ordering.by((entry: (String, (Int))) => entry._2)
      }.map(x => (x._1, x._2)).toList

      val top_labs = similar.filter(x => x.dstAttr.isInstanceOf[ObservationProperty])
      .map(x => (x.srcId, x.dstAttr.asInstanceOf[ObservationProperty].observation_concept_name))
      .distinct  
      .map(x => (x._2, (1))).reduceByKey(_ + _)
      .top(5) 
      {
          Ordering.by((entry: (String, (Int))) => entry._2)
      }.map(x => (x._1, x._2)).toList

      val top_race = similar.filter(x => x.dstAttr.isInstanceOf[RaceProperty])
      .map(x => (x.srcId, x.dstAttr.asInstanceOf[RaceProperty].race_concept_name))
      .distinct  
      .map(x => (x._2, (1))).reduceByKey(_ + _)
      .top(5) 
      {
          Ordering.by((entry: (String, (Int))) => entry._2)
      }.map(x => (x._1, x._2)).toList

      val top_gender = similar.filter(x => x.dstAttr.isInstanceOf[GenderProperty])
      .map(x => (x.srcId, x.dstAttr.asInstanceOf[GenderProperty].gender_concept_name))
      .distinct  
      .map(x => (x._2, (1))).reduceByKey(_ + _)
      .top(5) 
      {
          Ordering.by((entry: (String, (Int))) => entry._2)
      }.map(x => (x._1, x._2)).toList

      val top_age = similar.filter(x => x.dstAttr.isInstanceOf[AgeProperty])
      .map(x => (x.srcId, x.dstAttr.asInstanceOf[AgeProperty].age_range.toInt))
      .distinct  
      .map(x => (x._2, (1))).reduceByKey(_ + _)
      .top(5) 
      {
          Ordering.by((entry: (Int, (Int))) => entry._2)
      }.map(x => ((-1 * x._1), x._2)).toList

      (top_diagnoses, top_medications, top_labs, top_race, top_gender, top_age)
  }
}
